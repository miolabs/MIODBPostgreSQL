// swift-tools-version:5.3
// The swift-tools-version declares the minimum version of Swift required to build this package.

import PackageDescription

let package = Package(
    name: "MIODBPostgreSQL",
    platforms: [.macOS(.v10_15)],
    products: [
        .library(
            name: "MIODBPostgreSQL",
            targets: ["MIODBPostgreSQL"]),
    ],
    dependencies: [
        .package(url: "https://github.com/miolabs/MIODB.git", .branch("master")),
        .package(url: "https://github.com/codewinsdotcom/PostgresClientKit", from: "1.0.0"),
    ],
    targets: [
        .target(
            name: "MIODBPostgreSQL",
            dependencies: ["MIODB", "PostgresClientKit"]),
        .testTarget(
            name: "MIODBPostgreSQLTests",
            dependencies: ["MIODBPostgreSQL"]),
    ]
)
