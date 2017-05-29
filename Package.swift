import PackageDescription

let package = Package(
  name: "ZeeQL3PG",

  targets: [
    Target(name: "PostgreSQLAdaptor"),
  ],
  
  dependencies: [
    .Package(url: "git@github.com:helje5/CLibPQ.git", majorVersion: 0),
    .Package(url: "git@github.com:helje5/ZeeQL3.git", majorVersion: 0)
  ],
	
  exclude: [
    "ZeeQL3PG.xcodeproj",
    "GNUmakefile",
    "LICENSE",
    "README.md",
    "xcconfig"
  ]
)
