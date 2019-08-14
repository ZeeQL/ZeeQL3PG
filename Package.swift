// swift-tools-version:4.2

import PackageDescription

let package = Package(
  name: "ZeeQL3PG",

  products: [ // TBD: Use ZeeQL3 as library name?
    .library(name: "PostgreSQLAdaptor", targets: [ "PostgreSQLAdaptor" ])
  ],
  dependencies: [
    .package(url: "https://github.com/ZeeQL/CLibPQ.git", from: "2.0.1"),
    .package(url: "https://github.com/ZeeQL/ZeeQL3.git", from: "0.8.0")
  ],
  targets: [
    .target(name: "PostgreSQLAdaptor", dependencies: [ "CLibPQ", "ZeeQL" ])
  ]
)
