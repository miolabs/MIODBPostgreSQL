# MIODB PostgreSQL

PostgreSQL plugin for the MIODB framework. It enables you to use MIODB to manipulate data in a PostgreSQL database.


# PostgreSQL client installation
To use MIODB PostgreSQL you must have the appropriate PostgreSQL C-language client installed.

### Linux
```
$ sudo apt-get install libpq-dev
```

### macOS
```
$ brew install libpq
```

##### NOTES: 

**libpq** doesn't come with **postgresql** but **brew** doesn't make any symbolic link in /usr/local/lib or /usr/local/include folder to avoid conflicts in case you already have a **postgresql** installation so in order to compile in Xcode you should have to run ( if you do not have **postgresql** installed ):
```
$ brew link --force lippq
```

**open ssl** could be another issue. In modern **macOS**, **open ssl** came with the OS itself and **brew** can't link the headers, libs files or the pc file (pkg-config). The workaround is link the installed **brew** version of the **open ssl** pkg-config files manually:
```
$ ln -s /usr/local/opt/openssl/lib/pkgconfig/libssl.pc /usr/local/lib/pkgconfig/libssl.pc
$ ln -s /usr/local/opt/openssl/lib/pkgconfig/libcrypto.pc /usr/local/lib/pkgconfig/libcrypto.pc
```  

Be aware that sometimes the url that the openssl is installed is /usr/local/Cellar/openssl@3/lib/pkgconfig so change the path accordingly.

Test the installation with the comands to check that everything works fine.
```
$ pkg-config --libs libpq
$ pkg-config --cflags libpq
```
  
**MacOS** doesn't have **pkg-config** binary so if you need it install with:
```
$ brew install pkg-config
```

## Usage

#### Add dependencies

Add the `MIODBPostgreSQL` package to the dependencies within your application’s `Package.swift` file. Substitute `"x.x.x"` with the latest `MIODBPostgreSQL` [release](https://github.com/miolabs/MIODBPostgreSQL/releases).

```swift
.package(url: "https://github.com/miolabs/MIODBPostgreSQL.git", from: "x.x.x")
```

Add `MIODBPostgreSQL` to application's dependencies:

```swift
.target(name: "Application", dependencies: ["MIODBPostgreSQL"]),
```

#### Import package

```swift
import MIODBPostgreSQL
```

## Using MIODBPostgreSQL

```swift
let db = MDBPostgreConnection( host: host, port: port, user: user, password: password, database: db )
let products = try db.executeString( "SELECT * from products" )
```

## API Documentation

**TODO**


## Usefull link on how to setup C library in swift
https://theswiftdev.com/how-to-use-c-libraries-in-swift/

## License
This library is licensed under Apache 2.0. Full license text is available in [LICENSE](https://github.com/miolabs/MIODBPostgreSQL/blob/master/LICENSE.txt)
