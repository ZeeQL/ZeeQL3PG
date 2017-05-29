import PackageDescription

let package = Package(
  name: "ZeeQL3PG",

  targets: [
    Target(name: "PostgreSQLAdaptor"),
  ],
  
  dependencies: [
    .Package(url: "https://github.com/ZeeQL/CLibPQ.git",
             majorVersion: 1, minor: 0),
    .Package(url: "https://github.com/ZeeQL/ZeeQL3.git", majorVersion: 0)
  ],
	
  exclude: [
    "ZeeQL3PG.xcodeproj",
    "GNUmakefile",
    "LICENSE",
    "README.md",
    "xcconfig"
  ]
)
