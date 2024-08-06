// swift-tools-version: 6.0
import PackageDescription

let package = Package(
    name: "distributed-system",
    platforms: [.macOS(.v15)],
    dependencies: [
        .package(url: "https://github.com/apple/swift-argument-parser.git", from: "1.5.0"),
        .package(url: "https://github.com/apple/swift-nio.git", from: "2.70.0"),
    ],
    targets: [
        .executableTarget(
            name: "EchoSystem",
            dependencies: [
                .product(name: "ArgumentParser", package: "swift-argument-parser"),
                .product(name: "NIO", package: "swift-nio"),
                .product(name: "NIOFoundationCompat", package: "swift-nio"),
            ],
            swiftSettings: [.swiftLanguageVersion(.v6), .enableUpcomingFeature("ExistentialAny")]
        ),
        .testTarget(
            name: "EchoSystemTests",
            dependencies: ["EchoSystem"],
            swiftSettings: [.swiftLanguageVersion(.v6)]
        )
    ]
)
