// swift-tools-version: 6.0
import PackageDescription

let package = Package(
    name: "distributed-system",
    platforms: [.macOS(.v15)],
    dependencies: [
        .package(url: "https://github.com/apple/swift-nio.git", from: "2.70.0"),
        .package(url: "https://github.com/apple/swift-log.git", from: "1.6.1"),
        .package(url: "https://github.com/apple/swift-async-algorithms.git", from: "1.0.1"),
    ],
    targets: [
        .target(
            name: "EchoSystem",
            dependencies: [
                .product(name: "NIO", package: "swift-nio"),
                .product(name: "NIOFoundationCompat", package: "swift-nio"),
            ],
            swiftSettings: [.swiftLanguageMode(.v6), .enableUpcomingFeature("ExistentialAny")]
        ),
        .testTarget(
            name: "EchoSystemTests",
            dependencies: ["EchoSystem"],
            swiftSettings: [.swiftLanguageMode(.v6)]
        ),

        .target(
            name: "UniqueIDSystem",
            dependencies: [
                .product(name: "NIO", package: "swift-nio"),
                .product(name: "Logging", package: "swift-log"),

            ],
            swiftSettings: [.swiftLanguageMode(.v6), .enableUpcomingFeature("ExistentialAny")]
        ),
        .testTarget(
            name: "UniqueIDSystemTests",
            dependencies: ["UniqueIDSystem", .product(name: "AsyncAlgorithms", package: "swift-async-algorithms")],
            swiftSettings: [.swiftLanguageMode(.v6)]
        ),
    ]
)
