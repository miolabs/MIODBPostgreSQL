# MIODBPostgreSQL

PostgreSQL implementation for the MIODB ORM abstraction layer. This library enables you to use MIODB to manipulate data in a PostgreSQL database.

## Overview

MIODBPostgreSQL provides a Swift interface for connecting to and working with PostgreSQL databases. It implements the MIODB ORM abstraction layer, allowing for seamless integration with the MIODB framework which is located at `../MIODB`.

## Features

- PostgreSQL database connection management
- Query execution with automatic type conversion
- Support for PostgreSQL data types including:
  - Numeric types (integer, float, decimal)
  - Boolean
  - Text and VARCHAR
  - JSON and JSONB
  - Date and timestamp
  - UUID
- Schema management
- Connection pooling support
- Error handling
- Logging integration with MIOCoreLogger

## Requirements

- Swift 5.9 or later
- macOS 12.0 or later
- PostgreSQL
- libpq

## PostgreSQL Client Installation

To use MIODBPostgreSQL, you must have the appropriate PostgreSQL C-language client installed.

### Linux
```
$ sudo apt-get install libpq-dev
```

### macOS
```
$ brew install libpq
```

#### NOTES: 

**libpq** doesn't come with **postgresql** but **brew** doesn't make any symbolic link in /usr/local/lib or /usr/local/include folder to avoid conflicts in case you already have a **postgresql** installation. To compile in Xcode you may need to run (if you do not have **postgresql** installed):
```
$ brew link --force libpq
```

**openssl** could be another issue. In modern **macOS**, **openssl** comes with the OS itself and **brew** can't link the headers, libs files or the pc file (pkg-config). The workaround is to link the installed **brew** version of the **openssl** pkg-config files manually:
```
$ ln -s /usr/local/opt/openssl/lib/pkgconfig/libssl.pc /usr/local/lib/pkgconfig/libssl.pc
$ ln -s /usr/local/opt/openssl/lib/pkgconfig/libcrypto.pc /usr/local/lib/pkgconfig/libcrypto.pc
```  

Be aware that sometimes the path where openssl is installed is `/usr/local/Cellar/openssl@3/lib/pkgconfig` so change the path accordingly.

Test the installation with these commands to check that everything works fine:
```
$ pkg-config --libs libpq
$ pkg-config --cflags libpq
```
  
**macOS** doesn't have **pkg-config** binary by default, so if you need it, install with:
```
$ brew install pkg-config
```

## Installation

### Swift Package Manager

Add MIODBPostgreSQL to your `Package.swift` file:

```swift
.package(url: "https://github.com/miolabs/MIODBPostgreSQL.git", branch: "master")
```

Then add the dependency to your target:

```swift
.target(
    name: "YourTarget",
    dependencies: ["MIODBPostgreSQL"]
)
```

## Usage

### Basic Database Operations

```swift
import MIODBPostgreSQL

// Create a connection
let connection = MDBPostgreConnection()
let db = try connection.create("your_database")

// Configure connection properties
db.host = "localhost"
db.port = 5432
db.user = "postgres"
db.password = "your_password"

// Connect
try db.connect()

// Execute a query
let results = try db.executeQueryString("SELECT * FROM users")

// Disconnect when done
db.disconnect()
```

### Alternative Connection Method

```swift
let db = MDBPostgreConnection(host: "localhost", port: 5432, user: "postgres", password: "your_password", database: "your_database")
let products = try db.executeString("SELECT * from products")
```

### Using with MIODB ORM

MIODBPostgreSQL is designed to work with the MIODB ORM framework. See the MIODB documentation for more information on how to use the ORM features.

## License

This library is licensed under Apache 2.0. Full license text is available in [LICENSE](https://github.com/miolabs/MIODBPostgreSQL/blob/master/LICENSE.txt)