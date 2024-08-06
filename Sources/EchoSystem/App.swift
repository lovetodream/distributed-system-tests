import ArgumentParser
import Distributed
import NIOCore
import NIOConcurrencyHelpers
import NIOFoundationCompat
import NIOPosix
import Synchronization

import struct Foundation.URL
import class Foundation.JSONEncoder
import class Foundation.JSONDecoder

@main
struct App: AsyncParsableCommand {
    mutating func run() async throws {
        let system1 = try await System(id: 1)
        let system2 = try await System(id: 2)
        try await withThrowingDiscardingTaskGroup { group in
            group.addTask {
                try await system1.run()
            }

            group.addTask {
                try await system2.run()
            }

            let echo = try Echo.resolve(id: .init(id: 1, systemID: 2), using: system1)
            print(try await echo.echo("Hello World!"))

            group.cancelAll()
        }
    }
}

typealias DefaultDistributedActorSystem = System

distributed actor Echo: DistributedActor {
    distributed func echo(_ message: String) -> String {
        return message
    }
}

final class WeakActor {
    weak var base: (any DistributedActor)?

    init(_ base: (any DistributedActor)?) {
        self.base = base
    }
}

#if !hasFeature(Mutex)
/// Can be removed as soon as Mutex is in the Xcode Beta Toolchain.
///
/// Currently uses `NIOLock` for concurrency safe access to `Value`.
struct Mutex<Value: ~Copyable>: ~Copyable {
    private let lock = NIOLock()
    private let value: Box

    final class Box {
        var base: Value

        init(_ base: consuming Value) {
            self.base = base
        }
    }

    init(_ initialValue: consuming sending Value) {
        value = .init(initialValue)
    }
}

extension Mutex: @unchecked Sendable where Value: ~Copyable {
    borrowing func withLock<Result: ~Copyable, E: Error>(
        _ body: (inout sending Value) throws(E) -> sending Result
    ) throws(E) -> sending Result {
        lock.lock()
        defer { lock.unlock() }
        return try body(&value.base)
    }
}
#endif

struct Client: Sendable {
    let path: String

    func withChannel<Result>(
        id: Int,
        _ body: (NIOAsyncChannelInboundStream<ByteBuffer>, NIOAsyncChannelOutboundWriter<ByteBuffer>) async throws -> Result
    ) async throws -> Result {
        let client = try await ClientBootstrap(group: .singletonMultiThreadedEventLoopGroup)
            .connect(unixDomainSocketPath: "\(path)\(id)") { channel in
                channel.eventLoop.makeCompletedFuture {
                    return try NIOAsyncChannel<ByteBuffer, ByteBuffer>(
                        wrappingChannelSynchronously: channel
                    )
                }
            }

        return try await client.executeThenClose(body)
    }
}

final class System: DistributedActorSystem {

    let systemID: Int

    typealias SerializationRequirement = Codable
    typealias InvocationDecoder = SystemDecoder
    typealias InvocationEncoder = SystemEncoder
    typealias ResultHandler = SystemResultHandler

    let server: NIOAsyncChannel<NIOAsyncChannel<ByteBuffer, ByteBuffer>, Never>
    let client: Client

    let nextActorID = Atomic(0)
    let actors: Mutex<[ActorID: WeakActor]> = Mutex([:])

    struct ActorID: Identifiable, Hashable {
        let id: Int
        let systemID: Int
    }

    init(id: Int) async throws {
        let socketPath = URL.downloadsDirectory.appending(path: "sock").path()
        let server = try await ServerBootstrap(group: .singletonMultiThreadedEventLoopGroup)
            .bind(unixDomainSocketPath: "\(socketPath)\(id)", cleanupExistingSocketFile: true) { channel in
                channel.eventLoop.makeCompletedFuture {
                    try NIOAsyncChannel<ByteBuffer, ByteBuffer>(wrappingChannelSynchronously: channel)
                }
            }

        self.systemID = id
        self.server = server
        self.client = .init(path: socketPath)
    }

    func run() async throws {
        try await withThrowingDiscardingTaskGroup { group in
            try await server.executeThenClose { inbound, outbound in
                for try await connection in inbound {
                    group.addTask {
                        try await connection.executeThenClose { inbound, outbound in
                            for try await packet in inbound {
                                try await outbound.write(packet)
                            }
                            outbound.finish()
                        }
                    }
                }
            }
        }
    }

    func assignID<Act>(_ actorType: Act.Type) -> ActorID where Act : DistributedActor, ActorID == Act.ID {
        let actorID = nextActorID.wrappingAdd(1, ordering: .relaxed).newValue
        return .init(id: actorID, systemID: systemID)
    }

    func resignID(_ id: ActorID) {
        actors.withLock { _ = $0.removeValue(forKey: id) }
    }

    func actorReady<Act>(_ actor: Act) where Act : DistributedActor, ActorID == Act.ID {
        actors.withLock { $0[actor.id] = .init(actor) }
    }

    func resolve<Act>(id: ActorID, as actorType: Act.Type) throws -> Act? where Act : DistributedActor, ActorID == Act.ID {
        actors.withLock { $0[id]?.base as? Act }
    }

    func makeInvocationEncoder() -> SystemEncoder {
        SystemEncoder()
    }

    func remoteCall<Actor: DistributedActor, Err: Error, Result: SerializationRequirement>(
        on actor: Actor,
        target: RemoteCallTarget,
        invocation: inout InvocationEncoder,
        throwing: Err.Type,
        returning: Result.Type
    ) async throws -> Result where Actor.ID == ActorID {
        // this is a very idiomatic way to do this
        var buffer = try await client.withChannel(id: actor.id.systemID) { inbound, outbound in
            try await outbound.write(invocation.buffer)
            for try await value in inbound {
                return value
            }
            throw ChannelError.eof // just throwing some random error for now, eof seems okish
        }

        // the following cannot be nil as we're reading readable bytes, so there must be enough data readable
        return try buffer.readJSONDecodable(returning, length: buffer.readableBytes).unsafelyUnwrapped
    }

    func remoteCallVoid<Act, Err>(
        on actor: Act,
        target: RemoteCallTarget,
        invocation: inout SystemEncoder,
        throwing: Err.Type
    ) async throws where Act : DistributedActor, Err : Error, ActorID == Act.ID {
        fatalError()
    }
}

struct SystemEncoder: DistributedTargetInvocationEncoder {
    typealias SerializationRequirement = Codable

    private let encoder = JSONEncoder()
    var buffer = ByteBuffer()

    mutating func recordArgument<Value: SerializationRequirement>(_ argument: RemoteCallArgument<Value>) throws {
        let encoded = try encoder.encode(argument.value)
        buffer.writeBytes(encoded)
    }

    mutating func recordReturnType<Res: SerializationRequirement>(_ resultType: Res.Type) throws {
        // The caller functions return type can be encoded as part of the serialized packet, not yet sure in
        // which case this might be necessary. Maybe for method overloads?
    }

    mutating func recordGenericSubstitution<T>(_ type: T.Type) throws {
        fatalError()
    }
    
    mutating func recordArgument<Value>(_ argument: RemoteCallArgument<Value>) throws {
        fatalError()
    }
    
    mutating func recordErrorType<E>(_ type: E.Type) throws where E : Error {
        fatalError()
    }
    
    mutating func recordReturnType<R>(_ type: R.Type) throws {
        fatalError()
    }
    
    mutating func doneRecording() throws {
        // Seems to be called when encoding of a call is done. Might be applicable to bundle up the final packet here.
        // E.g. computing and prepending fields for checksum and packet length in the future.
    }
}

struct SystemDecoder: DistributedTargetInvocationDecoder {
    typealias SerializationRequirement = Codable

    mutating func decodeGenericSubstitutions() throws -> [any Any.Type] {
        fatalError()
    }
    
    mutating func decodeNextArgument<Argument: SerializationRequirement>() throws -> Argument {
        fatalError()
    }
    
    mutating func decodeErrorType() throws -> (any Any.Type)? {
        fatalError()
    }
    
    mutating func decodeReturnType() throws -> (any Any.Type)? {
        fatalError()
    }
}

struct SystemResultHandler: DistributedTargetInvocationResultHandler {
    typealias SerializationRequirement = Codable

    func onReturn<Success: SerializationRequirement>(value: Success) async throws {
        fatalError()
    }

    func onReturn<Success>(value: Success) async throws {
        fatalError()
    }
    
    func onReturnVoid() async throws {
        fatalError()
    }
    
    func onThrow<Err>(error: Err) async throws where Err : Error {
        fatalError()
    }
}
