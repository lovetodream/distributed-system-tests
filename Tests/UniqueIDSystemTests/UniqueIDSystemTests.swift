import AsyncAlgorithms
import Logging
import Synchronization
import Testing
import Foundation

@testable import UniqueIDSystem

let ids = Mutex(Set<UInt128>())
let runs = Atomic(0)

@Test func testUniqueIDs() async throws {
    LoggingSystem.bootstrap { label in
        var handler = StreamLogHandler.standardOutput(label: label)
        handler.logLevel = .debug
        return handler
    }

    let system1 = try await System(id: 1)
    let system2 = try await System(id: 2)
    let system3 = try await System(id: 3)
    let systems = [system1, system2, system3]

    let generator1 = UniqueID(actorSystem: system1)
    let generator2 = UniqueID(actorSystem: system2)
    let generator3 = UniqueID(actorSystem: system3)
    let generators = [generator1, generator2, generator3]

    let didRun = Atomic(false)
    try await withThrowingDiscardingTaskGroup { group in
        for system in systems {
            group.addTask {
                await #expect(throws: CancellationError.self, performing: {
                    try await system.run()
                })
            }
        }

        var passedSeconds = 0
        group.addTask { await tick(systems: systems, generators: generators) }
        for try await _ /* tick */ in AsyncTimerSequence(interval: .seconds(1), clock: ContinuousClock()) {
            passedSeconds += 1
            if passedSeconds == 30 {
                print("want to cancel pls")
                group.cancelAll()
                break
            }

            group.addTask { await tick(systems: systems, generators: generators) }
        }

        let result = ids.withLock { $0 }
        #expect(result.count == 1000 * 30)

        didRun.store(true, ordering: .relaxed)
    }

    let flag = didRun.load(ordering: .relaxed)
    #expect(flag)
}

func tick(systems: [System], generators: [UniqueID]) async {
    async let batchPromise = withThrowingTaskGroup(of: UInt128.self) { calls in
        for _ in 0..<1000 {
            let system = systems.randomElement().unsafelyUnwrapped
            let generatorID = generators.filter({ $0.actorSystem !== system }).randomElement().unsafelyUnwrapped.id
            let generator = try UniqueID.resolve(id: generatorID, using: system)
            calls.addTask {
                let id = try await generator.generate()
                return id
            }
        }

        var ids: Set<UInt128> = []
        for try await id in calls {
            #expect(ids.insert(id).inserted)
        }
        return ids
    }

    do {
        let batch = try await batchPromise
        ids.withLock({ $0.formUnion(batch) })
    } catch {
        print("[FATAL]", error)
    }

    print("\(runs.wrappingAdd(1, ordering: .relaxed).newValue)/30 finished")
}
