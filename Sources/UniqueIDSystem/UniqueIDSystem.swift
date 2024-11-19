import Distributed
import NIOCore
import NIOPosix
import Logging
import Synchronization

struct State: ~Copyable {
    var time: UInt64 = 0
    var count: UInt32 = 0

    mutating func next() -> (time: UInt64, count: UInt32) {
        let time = mach_absolute_time()
        let count = count + 1
        self.time = time
        self.count = count
        return (time, count)
    }
}

distributed actor UniqueID: DistributedActor {
    var state = State()

    distributed func generate() -> UInt128 {
        let systemID = actorSystem.systemID
        let (time, count) = state.next()
        var id = UInt128(time) << 64
        id += UInt128(count) << 32
        id += UInt128(systemID) << 0

        // extract time, count and systemID from id
        let extractedTime = id >> 64
        let extractedCount = (id >> 32) & 0xFFFFFFFF
        let extractedSystemID = id & 0xFFFFFFFF

        assert(extractedTime == time)
        assert(extractedCount == count)
        assert(extractedSystemID == systemID)

        return id
    }
}


// MARK: - System

typealias DefaultDistributedActorSystem = System

final class WriteResponseHandler: ChannelInboundHandler {
    typealias InboundIn = Envelope

    let continuation: AsyncStream<Envelope>.Continuation

    init(continuation: AsyncStream<Envelope>.Continuation) {
        self.continuation = continuation
    }

    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        let envelope = unwrapInboundIn(data)
        continuation.yield(envelope)
    }
}

struct EnvelopeEncoder: MessageToByteEncoder {
    func encode(data: Envelope, out: inout ByteBuffer) throws {
        switch data {
        case .invocation(let message):
            out.writeInteger(UInt8(1))
            out.writeInteger(message.callID)
            out.writeInteger(message.origin)
            out.writeInteger(message.recipient.id)
            out.writeInteger(message.recipient.systemID)
            try out.writeLengthPrefixed(as: UInt16.self) { $0.writeString(message.targetIdentifier) }
            try out.writeLengthPrefixed(as: UInt32.self) { $0.writeImmutableBuffer(message.payload) }
        case .result(var message):
            out.writeInteger(UInt8(2))
            out.writeInteger(message.callID)
            out.writeInteger(message.recipient)
            try out.writeLengthPrefixed(as: UInt32.self) { $0.writeBuffer(&message.result) }
        }
    }
}

struct EnvelopeDecoder: NIOSingleStepByteToMessageDecoder {
    func decode(buffer: inout ByteBuffer) throws -> Envelope? {
        let startReaderIndex = buffer.readerIndex
        guard let messageID: UInt8 = buffer.readInteger() else { return nil }
        switch messageID {
        case 1:
            let callID: UInt16? = buffer.readInteger()
            let originSystemID: UInt32? = buffer.readInteger()
            let recipientActorID: UInt32? = buffer.readInteger()
            let recipientSystemID: UInt32? = buffer.readInteger()
            let identifier = try buffer.readLengthPrefixed(as: UInt16.self) {
                $0.getString(at: $0.readerIndex, length: $0.readableBytes)
            }
            let payload = buffer.readLengthPrefixedSlice(as: UInt32.self)
            guard
                let callID,
                let originSystemID,
                let recipientActorID,
                let recipientSystemID,
                let identifier,
                let payload
            else {
                buffer.moveReaderIndex(to: startReaderIndex)
                return nil
            }
            return .invocation(.init(
                callID: callID,
                origin: originSystemID,
                recipient: .init(id: recipientActorID, systemID: recipientSystemID),
                targetIdentifier: identifier,
                payload: payload
            ))
        case 2:
            let callID: UInt16? = buffer.readInteger()
            let recipientSystemID: UInt32? = buffer.readInteger()
            let result = buffer.readLengthPrefixedSlice(as: UInt32.self)
            guard let callID, let recipientSystemID, let result else {
                buffer.moveReaderIndex(to: startReaderIndex)
                return nil
            }
            return .result(.init(
                callID: callID,
                recipient: recipientSystemID,
                result: result
            ))
        default:
            print(#function, "Invalid messageID: \(messageID)")
//            fatalError("Invalid messageID: \(messageID)")
            return nil
        }
    }

    func decodeLast(buffer: inout ByteBuffer, seenEOF: Bool) throws -> Envelope? {
        try decode(buffer: &buffer)
    }
}

enum Envelope {
    case invocation(InvocationMessage)
    case result(InvocationResult)

    var receivingSystem: UInt32 {
        switch self {
        case .invocation(let message):
            message.recipient.systemID
        case .result(let message):
            message.recipient
        }
    }
}

struct InvocationMessage {
    let callID: UInt16
    let origin: UInt32
    let recipient: System.ActorID
    let targetIdentifier: String
    let payload: ByteBuffer
}

struct InvocationResult {
    let callID: UInt16
    let recipient: UInt32
    var result: ByteBuffer
}

final class WeakActor {
    weak var base: (any DistributedActor)?

    init(_ base: (any DistributedActor)?) {
        self.base = base
    }
}

final class System: DistributedActorSystem {

    typealias AsyncChannel = NIOAsyncChannel<Envelope, Envelope>

    let systemID: UInt32

    typealias SerializationRequirement = BufferCodable
    typealias InvocationDecoder = SystemDecoder
    typealias InvocationEncoder = SystemEncoder
    typealias ResultHandler = SystemResultHandler

    let server: NIOAsyncChannel<AsyncChannel, Never>
    let logger: Logger

    let nextActorID = Atomic(UInt32(0))
    let actors: Mutex<[ActorID: WeakActor]> = Mutex([:])

    let nextCallID = Atomic(UInt16(0))
    let inflightCalls: Mutex<[UInt16: CheckedContinuation<InvocationResult, any Error>]> = Mutex([:])

    let invocationStream: AsyncStream<(Envelope, (NIOAsyncChannelOutboundWriter<Envelope>)?)>
    let pendingInvocations: AsyncStream<(Envelope, (NIOAsyncChannelOutboundWriter<Envelope>)?)>.Continuation

    let resultsStream: AsyncStream<Envelope>
    let resultsContinuation: AsyncStream<Envelope>.Continuation

    let openConnections: Mutex<[UInt32: any Channel]> = Mutex([:])

    struct ActorID: Identifiable, Hashable {
        let id: UInt32
        let systemID: UInt32
    }

    init(id: UInt32) async throws {
        let server = try await ServerBootstrap(group: .singletonMultiThreadedEventLoopGroup)
            .serverChannelOption(.backlog, value: 256)
            .serverChannelOption(.socketOption(.so_reuseaddr), value: 1)
            .childChannelOption(.tcpOption(.tcp_nodelay), value: 1)
            .childChannelOption(.socketOption(.so_reuseaddr), value: 1)
            .childChannelOption(.maxMessagesPerRead, value: 16)
            .childChannelOption(.recvAllocator, value: AdaptiveRecvByteBufferAllocator())
            .bind(host: "127.0.0.1", port: 8080 + Int(id)) { channel in
                channel.eventLoop.makeCompletedFuture {
                    try channel.pipeline.syncOperations.addHandler(MessageToByteHandler(EnvelopeEncoder()), name: "server-enc")
                    try channel.pipeline.syncOperations.addHandler(ByteToMessageHandler(EnvelopeDecoder()), name: "server-dec")
                    return try AsyncChannel(wrappingChannelSynchronously: channel)
                }
            }

        self.systemID = id
        self.logger = Logger(label: "System-\(id)")
        self.server = server

        let (stream, continuation) = AsyncStream<(Envelope, (NIOAsyncChannelOutboundWriter<Envelope>)?)>.makeStream()
        self.invocationStream = stream
        self.pendingInvocations = continuation

        let (resultsStream, resultsContinuation) = AsyncStream<Envelope>.makeStream()
        self.resultsStream = resultsStream
        self.resultsContinuation = resultsContinuation
    }

    func assignID<Act>(_ actorType: Act.Type) -> ActorID where Act : DistributedActor, ActorID == Act.ID {
        let actorID = nextActorID.wrappingAdd(1, ordering: .relaxed).newValue
        return .init(id: actorID, systemID: systemID)
    }

    func resignID(_ id: ActorID) {
        actors.withLock { _ = $0.removeValue(forKey: id) }
    }

    func actorReady<Act>(_ actor: Act)
    where Act : DistributedActor, ActorID == Act.ID {
        actors.withLock { $0[actor.id] = .init(actor) }
    }

    func resolve<Act>(id: ActorID, as actorType: Act.Type) throws -> Act? where Act : DistributedActor, ActorID == Act.ID {
        actors.withLock { $0[id]?.base as? Act }
    }

    func makeInvocationEncoder() -> SystemEncoder {
        SystemEncoder(logger: self.logger)
    }

    func remoteCall<Actor: DistributedActor, Err: Error, Result: SerializationRequirement>(
        on actor: Actor,
        target: RemoteCallTarget,
        invocation: inout InvocationEncoder,
        throwing: Err.Type,
        returning: Result.Type
    ) async throws -> Result where Actor.ID == ActorID {
        let callID = nextCallID.wrappingAdd(1, ordering: .relaxed).newValue
        var result: InvocationResult = try await withCheckedThrowingContinuation { continuation in
            inflightCalls.withLock { $0[callID] = continuation }
            let message = invocation.serialized(
                callID: callID,
                origin: self.systemID,
                recipient: actor.id,
                targetIdentifier: target.identifier
            )
            logger.trace("Sending call invocation to \(actor.id)", metadata: [
                "target": "\(target.identifier)",
                "callID": "\(callID)",
                "origin": "\(self.systemID)"
            ])
            pendingInvocations.yield((.invocation(message), nil))
        }
        precondition(callID == result.callID)
        inflightCalls.withLock { _ = $0.removeValue(forKey: callID) }
        return try Result.decode(from: &result.result)
    }

    func remoteCallVoid<Act, Err>(
        on actor: Act,
        target: RemoteCallTarget,
        invocation: inout SystemEncoder,
        throwing: Err.Type
    ) async throws where Act : DistributedActor, Err : Error, ActorID == Act.ID {
        print(#function)
//        fatalError()
    }
}


// MARK: Lifecycle

extension System {

    func run() async throws {
        try await withTaskCancellationHandler {
            try await withThrowingDiscardingTaskGroup { group in
                group.addTask {
                    await self.outgoingTraffic()
                }

                group.addTask {
                    for try await envelope in self.resultsStream {
                        try await self.handleEnvelope(envelope, replyOn: nil)
                    }
                }

                group.addTask {
                    try await self.serverTraffic()
                }
            }
        } onCancel: { // FIXME: improve cleanup
            inflightCalls.withLock { $0.forEach { $0.value.resume(throwing: CancellationError()) } }
            openConnections.withLock { $0.forEach { $0.value.close(promise: nil) } }
        }
    }

    /// Returns a channel, either from cache or a fresh channel.
    ///
    /// - Note: If the cache lock is currently acquired, a new connection will be established. This prevents long wait times when running concurrent calls.
    private func channel(for target: UInt32) async throws -> any Channel {
        if let channel = openConnections.withLock({ $0[target] }), channel.isActive {
            return channel
        }

        let channel = try await ClientBootstrap(group: .singletonMultiThreadedEventLoopGroup)
            .channelOption(.socketOption(.so_reuseaddr), value: 1)
            .connect(host: "127.0.0.1", port: 8080 + Int(target)) { channel in
                channel.eventLoop.makeCompletedFuture {
                    try channel.pipeline.syncOperations.addHandler(MessageToByteHandler(EnvelopeEncoder()), name: "client-enc")
                    try channel.pipeline.syncOperations.addHandler(ByteToMessageHandler(EnvelopeDecoder()), name: "client-dec")
                    try channel.pipeline.syncOperations.addHandler(WriteResponseHandler(continuation: self.resultsContinuation))
                    return channel
                }
            }
        openConnections.withLock({ $0[target] = channel })
        return channel
    }

    private func outgoingTraffic() async {
        await withDiscardingTaskGroup { group in
            for await message in self.invocationStream {
                group.addTask {
                    do {
                        if let writer = message.1 {
                            try await writer.write(message.0)
                            return
                        }
                    } catch {
                        self.logger.warning(
                            "Failed to send message on existing writer, will try new channel.",
                            metadata: ["error": "\(error)"]
                        )
                    }
                    while true { // FIXME: improve!
                        do {
                            let channel = try await self.channel(for: message.0.receivingSystem)
                            try await channel.writeAndFlush(message.0)
                            break
                        } catch {
                            self.logger.warning("Failed to create channel for message.", metadata: ["error": "\(error)", "timeout": "100ms"])
                            do {
                                try await Task.sleep(for: .milliseconds(100))
                            } catch {
                                break
                            }
                        }
                    }
                }
            }
        }
    }

    private func serverTraffic() async throws {
        try await withThrowingDiscardingTaskGroup { group in
            try await self.server.executeThenClose { inbound in
                for try await connection in inbound {
                    group.addTask {
                        do {
                            try await connection.executeThenClose { inbound, outbound in
                                for try await envelope in inbound {
                                    try await self.handleEnvelope(envelope, replyOn: outbound)
                                }
                            }
                            self.logger.trace("Closed connection to client.")
                        } catch is CancellationError {
                            self.logger.trace("Connection to client cancelled.")
                        } catch {
                            self.logger.warning("Failure in connection to client.", metadata: ["error": "\(error)"])
                        }
                    }
                }
            }
        }
    }

    private func handleEnvelope(_ envelope: Envelope, replyOn writer: NIOAsyncChannelOutboundWriter<Envelope>?) async throws {
        switch envelope {
        case .invocation(let message):
            let actor = self.actors.withLock({ $0[message.recipient]!.base! })
            var decoder = SystemDecoder(logger: self.logger, payload: message.payload)
            let handler = SystemResultHandler(
                logger: self.logger,
                continuation: self.pendingInvocations,
                writer: writer,
                callID: message.callID,
                recipient: message.origin
            )
            try await self.executeDistributedTarget(
                on: actor,
                target: RemoteCallTarget(message.targetIdentifier),
                invocationDecoder: &decoder,
                handler: handler
            )
        case .result(let message):
            logger.trace("Received call result", metadata: [
                "callID": "\(message.callID)",
                "recipient": "\(message.recipient)"
            ])
            let continuation = self.inflightCalls.withLock { $0[message.callID] }! // TODO: check
            continuation.resume(returning: message)
        }
    }

}

struct SystemEncoder: DistributedTargetInvocationEncoder {
    typealias SerializationRequirement = BufferCodable

    let logger: Logger
    private var buffer = ByteBuffer()

    init(logger: Logger) {
        self.logger = logger
    }

    mutating func recordArgument<Value: SerializationRequirement>(_ argument: RemoteCallArgument<Value>) throws {
        logger.trace("\(#function)")
        fatalError()
    }

    mutating func recordReturnType<Res: SerializationRequirement>(_ resultType: Res.Type) throws {
        // The caller functions return type can be encoded as part of the serialized packet, not yet sure in
        // which case this might be necessary. Maybe for method overloads?
        logger.trace("\(#function)")
    }

    mutating func recordGenericSubstitution<T>(_ type: T.Type) throws {
        logger.trace("\(#function)")
        fatalError()
    }

    mutating func recordArgument<Value>(_ argument: RemoteCallArgument<Value>) throws {
        logger.trace("\(#function)")
        fatalError()
    }

    mutating func recordErrorType<E>(_ type: E.Type) throws where E : Error {
        logger.trace("\(#function)")
        fatalError()
    }

    mutating func recordReturnType<R>(_ type: R.Type) throws {
        logger.trace("\(#function)")
        fatalError()
    }

    mutating func doneRecording() throws {
        // Seems to be called when encoding of a call is done. Might be applicable to bundle up the final packet here.
        // E.g. computing and prepending fields for checksum and packet length in the future.
        logger.trace("\(#function)")
    }

    func serialized(callID: UInt16, origin: UInt32, recipient: System.ActorID, targetIdentifier: String) -> InvocationMessage {
        let message = InvocationMessage(
            callID: callID,
            origin: origin,
            recipient: recipient,
            targetIdentifier: targetIdentifier,
            payload: buffer
        )
        return message
    }
}

struct SystemDecoder: DistributedTargetInvocationDecoder {
    typealias SerializationRequirement = BufferCodable

    let logger: Logger
    var payload: ByteBuffer

    mutating func decodeGenericSubstitutions() throws -> [any Any.Type] {
        logger.trace("\(#function)")
        fatalError()
    }

    mutating func decodeNextArgument<Argument: SerializationRequirement>() throws -> Argument {
        logger.trace("\(#function)")
        fatalError()
    }

    mutating func decodeErrorType() throws -> (any Any.Type)? {
        logger.trace("\(#function)")
        fatalError()
    }

    mutating func decodeReturnType() throws -> (any Any.Type)? {
        // unused
        logger.trace("\(#function)")
        return nil
    }
}

struct SystemResultHandler: DistributedTargetInvocationResultHandler {
    typealias SerializationRequirement = BufferCodable

    let logger: Logger
    let continuation: AsyncStream<(Envelope, (NIOAsyncChannelOutboundWriter<Envelope>)?)>.Continuation
    let writer: NIOAsyncChannelOutboundWriter<Envelope>?
    let callID: UInt16
    let recipient: UInt32

    func onReturn<Success: SerializationRequirement>(value: Success) async throws {
        var buffer = ByteBuffer()
        try value.encode(into: &buffer)
        let result = InvocationResult(callID: callID, recipient: recipient, result: buffer)
        continuation.yield((.result(result), writer))
    }

    func onReturn<Success>(value: Success) async throws {
        logger.trace("\(#function)")
        fatalError()
    }

    func onReturnVoid() async throws {
        logger.trace("\(#function)")
        fatalError()
    }

    func onThrow<Err>(error: Err) async throws where Err : Error {
        logger.warning("\(#function) : \(error)")
        fatalError()
    }
}

protocol BufferCodable {
    func encode(into buffer: inout ByteBuffer) throws
    static func decode(from buffer: inout ByteBuffer) throws -> Self
}

extension UInt128: BufferCodable {
    func encode(into buffer: inout ByteBuffer) throws {
        buffer.writeInteger(self)
    }

    static func decode(from buffer: inout ByteBuffer) throws -> UInt128 {
        buffer.readInteger().unsafelyUnwrapped // TODO: throw
    }
}
