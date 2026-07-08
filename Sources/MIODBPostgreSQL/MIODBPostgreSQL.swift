//
//  MIODBPostgresSQL.swift
//  MIODB
//
//  Created by Javier Segura Perez on 24/12/2019.
//  Copyright © 2019 Javier Segura Perez. All rights reserved.
//

import Foundation
import MIOCore
import MIODB
@_implementationOnly import CLibPQ
import MIOCoreLogger


enum MIODBPostgreSQLError: Error {
    case fatalError( _ code:String, _ msg: String )
}

extension MIODBPostgreSQLError: LocalizedError {
    public var errorDescription: String? {
        switch self {
        case let .fatalError(code, msg):
            return "Fatal error. Code: \(code) Message: \"\(msg)\"."
        }
    }
}

open class MIODBPostgreSQL: MIODB
{
    let defaultPort:Int32 = 5432
    let defaultUser = "root"
    let defaultDatabase = "public"
    
    var _connection:OpaquePointer?
    var _connection_str: [CChar]?
    var _db:String? = nil
    
    deinit { disconnect() }
    
    open override func connect( _ to_db: String? = nil ) throws
    {
        if port == nil { port = defaultPort }
        if user == nil { user = defaultUser }
        
        _db = to_db ?? database
        // if database == nil { database = defaultDatabase }
                
        Log.debug( "ID: \(identifier). Connecting to POSTGRESQL Database. Connection string: \(host!):\(port!)/\(_db ?? defaultDatabase) \(scheme ?? "")" )
        let app_name = ( label + ( scheme != nil ? "#" + scheme! : "" ) ).replacingOccurrences(of: "[^a-zA-Z0-9.\\_\\-#]+", with: "_", options: .regularExpression)
        let options = scheme != nil ? " options='-c search_path=\(scheme!),public'" : ""
        // connect_timeout: bound the TCP connect attempt. Without this, libpq
        // will wait for the OS-level TCP timeout (~75-130s on Linux) when the
        // remote is unreachable, pinning the calling thread for that entire
        // duration. With many concurrent connect attempts this saturates the
        // server's NIOThreadPool.
        // keepalives: detect silently-dropped connections (NAT timeouts,
        // middleboxes, DB restarts) within ~60s instead of waiting until the
        // next query fails arbitrarily long after the connection went bad.
        let timeout_opts = " connect_timeout=5 keepalives=1 keepalives_idle=30 keepalives_interval=10 keepalives_count=3"
        connectionString = "host = \(host!) port = \(port!) user = \(user!) password = \(password!) dbname = \(_db ?? defaultDatabase) application_name = \(app_name)\(timeout_opts)\(options)"
        _connection_str = connectionString!.cString(using: .utf8)
        
        _connection = PQconnectdb( _connection_str )
        let status = PQstatus( _connection )
        if  status != CONNECTION_OK {
            _connection = nil
//            throw MIODBPostgreSQLError.fatalError("-1", "Could not connect to POSTGRESQL Database. Connection string: \(connectionString!)")
            Log.error( "ID: \(identifier). Could not connect to POSTGRESQL Database. host:\(host!), port: \(port!), dbname:\(_db ?? defaultDatabase) \(scheme ?? "")")
            throw MIODBPostgreSQLError.fatalError("-1", "Could not connect to POSTGRESQL Database.")
        }

        try super.connect( to_db )

        // statement_timeout: bound query execution time at the server side.
        // Without this, a query that hangs (advisory lock contention, slow
        // query plan, etc.) holds the calling thread until the client gives
        // up, which is never. 30s is a generous default; long-running
        // operations (migrations, bulk imports) should override per-session.
        // Configurable via MDB_POSTGRESQL_STATEMENT_TIMEOUT env var.
        let stmt_timeout = MCEnvironmentVar( "MDB_POSTGRESQL_STATEMENT_TIMEOUT" ) ?? "30s"
        _ = try? executeQuery( "SET statement_timeout = '\(stmt_timeout)'" )
    }
    
    open override func disconnect() {
        if _connection != nil {
            PQfinish( _connection )
            _connection = nil
            _connection_str = nil
            Log.debug( "ID: \(identifier). Diconnecting to POSTGRESQL Database. Connection string: \(host!):\(port!)/\(_db ?? defaultDatabase)." )
            super.disconnect( )
        }
    }

    /// Executes a query and returns a lazy result set. Rows keep the raw
    /// server response and convert each cell to its Swift value only when it
    /// is accessed, preserving the column order of the query. Prefer this
    /// over `executeQueryString` when not every column of every row is read.
    @discardableResult open override func executeQuery(_ query:String) throws -> MDBPostgreSQLResultSet {
        queryWillExecute() // To notify the pool idle time out wher about to start

        defer {
            queryDidExecute() // To notify the pool idle time out we don't nned the conneciton anymore
        }

        return try _executeQuery( query )
    }

    @discardableResult open func _executeQuery(_ query:String) throws -> MDBPostgreSQLResultSet {

        if ( PQstatus( _connection ) != CONNECTION_OK ) {
            Log.error( "ID: \(identifier). Postgres connection was lost, re-connecting and crossing fingers" )
            disconnect()
            usleep( 500000 ) // 0.5 seconds
            try connect()
        }

        Log.trace( "ID: \(identifier). QUERY: \(query)" )

        let res = query.withCString { PQexec( _connection, $0 ) }

        guard let res = res else {
            let msg = _connection != nil ? String(cString: PQerrorMessage(_connection)) : "Connection is nil"
            throw MIODBPostgreSQLError.fatalError("-3", (scheme != nil ? "\(scheme!): " : "") + msg + "\n" + query)
        }

        switch PQresultStatus(res) {

        case PGRES_COMMAND_OK, PGRES_TUPLES_OK: break

        case PGRES_FATAL_ERROR:
            let errorMessage = (scheme != nil ? "\(scheme!): " : "") + String(cString: PQresultErrorMessage(res)) + "\n" + query
            let err_code = PQresultErrorField(res, 67 )
            let code = err_code != nil ? String( cString: err_code! ) : "0"
            PQclear(res)
            Log.trace( "ID: \(identifier). \(errorMessage)" )
            throw MIODBPostgreSQLError.fatalError(code, errorMessage)

        case PGRES_EMPTY_QUERY    : Log.warning("Empty query")
        case PGRES_COPY_OUT       : Log.warning("Copy out")
        case PGRES_COPY_IN        : Log.warning("Copy in")
        case PGRES_BAD_RESPONSE   : Log.warning("Bad response")
        case PGRES_NONFATAL_ERROR : Log.warning("Non fatal error")
        case PGRES_COPY_BOTH      : Log.warning("Copy both")
        case PGRES_SINGLE_TUPLE   : Log.warning("Single tupple")

        default:
            Log.warning("ID: \(identifier). Response not implemented.")
        }

        // The result set owns the PGresult from here and clears it on deinit.
        return MDBPostgreSQLResultSet( result: res, db: self )
    }
    
    func convert ( value:UnsafePointer<Int8>, withType type:Oid ) throws -> Any {
        if type == 16 { // Boolean
            return (value[0] == 116) as Bool
        }

        // Integers parse straight off the C buffer — no String round-trip.
        switch type {
        case 20: return strtoll( value, nil, 10 )                          // Int8
        case 23: return Int32( truncatingIfNeeded: strtol( value, nil, 10 ) ) // Int4
        case 21: return Int16( truncatingIfNeeded: strtol( value, nil, 10 ) ) // Int2
        default: break
        }

        var ret:Any?
        let str = String(cString: value)

        switch type {
        // "char" (OID 18) is a 1-byte CHARACTER, not a number — the catalogs
        // use it as an enum letter (e.g. pg_constraint.contype = 'c', 'p', 'f').
        // Keep the numeric conversion for digit values, but fall back to the
        // string instead of crashing on non-numeric characters.
        case 18: ret = MIOCoreInt8Value( str ) ?? str
            
        case 1700, 700, 701, 790: // numeric, float4, float8, money
            // 'NaN' / 'Infinity' are valid float values but not valid Decimals
            ret = Decimal( string: str )
            
        case 1114: ret = convert_date( str, false ) // Timestamp
        case 1184: ret = convert_date( str, true ) // Timestamp Z
        case 1083: ret = str // TODO: Time

        case 1043: // varchar
            ret = str
        case 114, 3802: // json, jsonb (= transformable for us)
            ret = try JSONSerialization.jsonObject(with: str.data(using: .utf8)!, options: [.allowFragments] )
        case 3807: // json binary array
            ret = try JSONSerialization.jsonObject(with: str.data(using: .utf8)!, options: [.allowFragments] )

        case 2950: // UUID
            ret = UUID( uuidString: str )
            
        case 25,19: // Text, Name(used when getting information from the DB as which contraints/indices/etc has)
            ret = str
            
        case 1082: // date
            ret = MIOCoreDate( fromString: str )
            
        case 2278: // void
            ret = str
        default:
            Log.warning( "ID: \(identifier). Type not implemented. Fallback to string. type: \(type)" )
            ret = str
        }

        // Unparseable dates / UUIDs / decimals fall back to the raw string
        // instead of crashing the client.
        return ret ?? str
    }
    
    var date_formatter:ISO8601DateFormatter = {
        let formatter = ISO8601DateFormatter()
        formatter.formatOptions = [.withInternetDateTime, .withFractionalSeconds]
        return formatter
    }()
    
    func convert_date( _ str: String, _ timeZone: Bool ) -> Date? {
        let isoString = (timeZone ? str : str.appending( "+00"))
            .replacingOccurrences(of: " ", with: "T")
            .replacingOccurrences(of: "+00", with: "+00:00")
        if let d = date_formatter.microsecondsDate(from: isoString) { return d }
        if let d = MIOCoreDate(fromString: str) {
            Log.debug( "ID: \(identifier). Fallback to String. date: \(isoString)" )
            return d
        }
        Log.warning( "ID: \(identifier). date: \(isoString) -> Can't be converted to Date" )
        return nil
    }
    
    open override func changeScheme(_ scheme:String?) throws {
        if scheme == nil { return }
        try super.changeScheme( scheme )
        
        if _connection == nil {
            throw MIODBPostgreSQLError.fatalError("-2","Could not change the scheme. The connection is nil")
        }

        try executeQuery("SET search_path TO \(scheme!), public; SET application_name TO '\(app_name())'")
    }
    
    var app_name_env_var:String? = nil
    var app_name_var_ids:[String] = []
    func app_name() -> String {
        
        func ids_from_var_name( _ value: String, _ prefix:String, _ suffix:String ) -> [String] {
            // replace all enviroment var with prefix and suffic of char %
            let pattern = "\(prefix)(\\w+)\(suffix)"
            let regex = try! NSRegularExpression(pattern: pattern)
            let nsrange = NSRange(value.startIndex..<value.endIndex, in: value)
            let matches = regex.matches(in: value, range: nsrange)

            return matches.compactMap { match -> String? in
                guard let range = Range(match.range(at: 1), in: value) else { return nil }
                return String(value[range])
            }
        }
        
        if app_name_env_var == nil {
            if var value = MCEnvironmentVar("MDB_POSTGRESQL_APPNAME") {
                // replace all enviroment var with prefix and suffic of char %
                let env_ids = ids_from_var_name( value, "%", "%")
                for i in env_ids {
                    let env_var_name = MCEnvironmentVar( i ) ?? ""
                    value = value.replacingOccurrences( of: "%\(i)%", with: env_var_name )
                }
                
                app_name_var_ids = ids_from_var_name( value, "\\{", "\\}")
                app_name_env_var = value
            }
            else { app_name_env_var = "mdb-postgresql" }
        }
        
        var value = app_name_env_var!
        
        for i in app_name_var_ids {
            switch i {
            case "host"  : value = value.replacingOccurrences( of: "{\(i)}", with: host ?? "" )
            case "user"  : value = value.replacingOccurrences( of: "{\(i)}", with: user ?? "" )
            case "schema": value = value.replacingOccurrences( of: "{\(i)}", with: scheme ?? "" )
            default: break
            }
        }
                    
        return value
    }

    // MARK: - Diacritic-insensitive search helpers
    // TODO
}

