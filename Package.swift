// swift-tools-version:5.1
// The swift-tools-version declares the minimum version of Swift required to build this package.

import PackageDescription

let package = Package(
    name: "MIODBPostgreSQL",
    platforms: [
        .macOS(.v10_15), .iOS(.v11)
    ],
    products: [
        // Products define the executables and libraries produced by a package, and make them visible to other packages.
        .library(
            name: "MIODBPostgreSQL",
            targets: ["MIODBPostgreSQL"]),
    ],
    dependencies: [
        // Dependencies declare other packages that this package depends on.
        // .package(url: /* package url */, from: "1.0.0"),
        .package(url: "https://github.com/miolabs/MIODB.git", .branch("master"))
    ],
    targets: [
        // Targets are the basic building blocks of a package. A target can define a module or a test suite.
        // Targets can depend on other targets in this package, and on products in packages which this package depends on.
        .systemLibrary(
            name: "CLibPQ",
            pkgConfig: "libpq",
            providers: [
                .brew(["libpq"]),
                .apt(["libpq-dev"])
            ]
        ),
        .target(
            name: "MIODBPostgreSQL",
            dependencies: ["MIODB", "CLibPQ"]),
        .testTarget(
            name: "MIODBPostgreSQLTests",
            dependencies: ["MIODBPostgreSQL"]),
    ]
)
