import Synchronization
import Testing

@testable import EchoSystem

@Test func testEcho() async throws {
    let system1 = try await System(id: 1)
    let system2 = try await System(id: 2)
    let didRun = Atomic(false)
    do {
        try await withThrowingDiscardingTaskGroup { group in
            group.addTask {
                try await system1.run()
            }

            group.addTask {
                try await system2.run()
            }

            let echo = try Echo.resolve(id: .init(id: 1, systemID: 2), using: system1)
            let response = try await echo.echo("Hello World!")
            #expect(response == "Hello World!")

            didRun.store(true, ordering: .relaxed)

            group.cancelAll()
        }
    } catch is CancellationError { }

    let flag = didRun.load(ordering: .relaxed)
    #expect(flag)
}
