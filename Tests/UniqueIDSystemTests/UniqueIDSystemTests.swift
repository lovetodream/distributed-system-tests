import AsyncAlgorithms
import Logging
import Synchronization
import Testing

@testable import UniqueIDSystem

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
    do {
        try await withThrowingDiscardingTaskGroup { group in
            for system in systems {
                group.addTask {
                    try await system.run()
                }
            }

            var ids = Set<UInt128>()
            let runs = Atomic(0)
            for try await _ /* tick */ in AsyncTimerSequence(interval: .seconds(1), clock: ContinuousClock()) {
                print("\(runs.wrappingAdd(1, ordering: .relaxed).newValue)/30s")

                let batch = try await withThrowingTaskGroup(of: UInt128.self) { group in
                    for _ in 0..<1000 {
                        let system = systems.randomElement().unsafelyUnwrapped
                        let generatorID = generators.filter({ $0.actorSystem !== system }).randomElement().unsafelyUnwrapped.id
                        let generator = try UniqueID.resolve(id: generatorID, using: system)
                        group.addTask {
                            let id = try await generator.generate()
                            return id
                        }
                    }

                    var ids: Set<UInt128> = []
                    for try await id in group {
                        #expect(ids.insert(id).inserted)
                    }
                    return ids
                }
                ids.formUnion(batch)

                if runs.load(ordering: .relaxed) == 30 {
                    print("want to cancel pls")
                    group.cancelAll()
                    break
                }
            }

            #expect(ids.count == 1000 * 30)

            didRun.store(true, ordering: .relaxed)
        }
    } catch is CancellationError { }

    let flag = didRun.load(ordering: .relaxed)
    #expect(flag)
}
