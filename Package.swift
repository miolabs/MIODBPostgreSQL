// swift-tools-version:5.3
// The swift-tools-version declares the minimum version of Swift required to build this package.

import PackageDescription

let package = Package(
    name: "MIODBPostgreSQL",
    platforms: [.macOS(.v10_15)],
    products: [
<<<<<<< HEAD
        .library(
            name: "MIODBPostgreSQL",
            targets: ["MIODBPostgreSQL"]),
    ],
    dependencies: [
        .package(url: "https://github.com/miolabs/MIODB.git", .branch("master")),
        .package(url: "https://github.com/codewinsdotcom/PostgresClientKit", from: "1.0.0"),
=======
        // Products define the executables and libraries produced by a package, and make them visible to other packages.
        .library( name: "MIODBPostgreSQL", targets: ["MIODBPostgreSQL"]),
    ],
    dependencies: [
        // Dependencies declare other packages that this package depends on.
        // .package(url: /* package url */, from: "1.0.0"),
        .package(url: "https://github.com/miolabs/MIODB.git", .branch("master") )
>>>>>>> master
    ],
    targets: [
        .target(
            name: "MIODBPostgreSQL",
<<<<<<< HEAD
            dependencies: ["MIODB", "PostgresClientKit"]),
=======
            dependencies: [
                .product(name: "MIODB", package: "MIODB"),
                "CLibPQ",
            ]),
>>>>>>> master
        .testTarget(
            name: "MIODBPostgreSQLTests",
            dependencies: ["MIODBPostgreSQL"]),
    ]
)
