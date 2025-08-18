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
import CLibPQ
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
        connectionString = "host = \(host!) port = \(port!) user = \(user!) password = \(password!) dbname = \(_db ?? defaultDatabase) application_name = \(app_name)\(options)"
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
    
    @discardableResult open override func executeQueryString(_ query:String) throws -> [[String : Any]]? {
        queryWillExecute() // To notify the pool idle time out wher about to start
        
        defer {
            queryDidExecute() // To notify the pool idle time out we don't nned the conneciton anymore
        }
        
        var items:[[String : Any]]?
        try MIOCoreAutoReleasePool {
            items = try _executeQueryString(query)
        }
        
        return items
    }
    
    @discardableResult open func _executeQueryString(_ query:String) throws -> [[String : Any]]? {
                
        if ( PQstatus( _connection ) != CONNECTION_OK ) {
            Log.error( "ID: \(identifier). Postgres connection was lost, re-connecting and crossing fingers" )
            disconnect()
            usleep( 500000 ) // 0.5 seconds
            try connect()
        }
        
        Log.trace( "ID: \(identifier). QUERY: \(query)" )
        
        let r = try query.withCString { query_cstr -> [[String : Any]] in
            let res = PQexec( _connection, query_cstr )
            
            defer { PQclear(res) }
            
            var items:[[String : Any]] = []
            
            switch PQresultStatus(res) {
                
            case PGRES_COMMAND_OK: break
                
            case PGRES_TUPLES_OK:
                for row in 0..<PQntuples(res) {
                    var item: [String:Any] = [:]
                    for col in 0..<PQnfields(res){
                        let colname = String(cString: PQfname(res, col))
                        
                        if PQgetisnull(res, row, col) == 1 {
                            item[ colname ] = NSNull( )
                            continue
                        }
                        
                        let type = PQftype(res, col)
                        let value = PQgetvalue(res, row, col)
                        
                        item[colname] = try convert(value: value!, withType: type)
                    }
                    items.append(item)
                }
                
            case PGRES_FATAL_ERROR:
                let errorMessage = (scheme != nil ? "\(scheme!): " : "") + String(cString: PQresultErrorMessage(res)) + "\n" + query
                let err_code = PQresultErrorField(res, 67 )
                let code = err_code != nil ? String( cString: err_code! ) : "0"
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
                                    
            return items
        }
                
        Log.trace( "ID: \(identifier). RESPONSE: \(r)" )
        return r
    }
    
    func convert ( value:UnsafePointer<Int8>, withType type:Oid ) throws -> Any {
        if type == 16 { // Boolean
            return (value[0] == 116) as Bool
        }
        
        var ret:Any?
        let str = String(cString: value)

        switch type {

        case 20: ret = MIOCoreInt64Value( str )! // Int8
        case 23: ret = MIOCoreInt32Value( str )! // Int4
        case 21: ret = MIOCoreInt16Value( str )! // Int2
        case 18: ret = MIOCoreInt8Value(  str )! // UInt8, char
            
        case 1700, 700, 701, 790: // numeric, float4, float8, money
            ret = Decimal( string: str )!
            
        case 1114: ret = MIOCoreDate( fromString: str ) // Timestamp
        case 1184: ret = MIOCoreDate( fromString: str ) // Timestamp Z
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
        
        return ret!
    }
    
    open override func changeScheme(_ scheme:String?) throws {
        if scheme == nil { return }
        try super.changeScheme( scheme )
        
        if _connection == nil {
            throw MIODBPostgreSQLError.fatalError("-2","Could not change the scheme. The connection is nil")
        }

        try executeQueryString("SET search_path TO \(scheme!), public; SET application_name TO '\(app_name())'")
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

    /// Executes a query with diacritic-insensitive text search using unaccent extension
    /// - Parameters:
    ///   - table: The table name to search in
    ///   - column: The column name to search
    ///   - searchText: The text to search for
    ///   - exactMatch: If true, uses = operator, otherwise uses ILIKE for partial matching
    /// - Returns: Query results
    @discardableResult open func searchIgnoringDiacritics(
        table: String,
        column: String,
        searchText: String,
        exactMatch: Bool = false
    ) throws -> [[String : Any]]? {
        let escapedSearchText = searchText.replacingOccurrences(of: "'", with: "''")
        let sqlOperator = exactMatch ? "=" : "ILIKE"
        let searchPattern = exactMatch ? "'\(escapedSearchText)'" : "'%\(escapedSearchText)%'"

        let query = """
            SELECT * FROM \(table)
            WHERE unaccent(\(column)) \(sqlOperator) unaccent(\(searchPattern))
            """

        return try executeQueryString(query)
    }

    /// Ensures the unaccent extension is available in the database
    /// Call this once after connecting to enable diacritic-insensitive searches
    open func enableUnaccentExtension() throws {
        _ = try executeQueryString("CREATE EXTENSION IF NOT EXISTS unaccent")
    }

    /// Creates a custom text search configuration for diacritic-insensitive full-text search
    /// - Parameter configName: Name for the custom configuration (default: "simple_unaccent")
    open func createUnaccentTextSearchConfig(configName: String = "simple_unaccent") throws {
        let queries = [
            "CREATE TEXT SEARCH CONFIGURATION \(configName) (COPY = simple)",
            """
            ALTER TEXT SEARCH CONFIGURATION \(configName)
              ALTER MAPPING FOR asciiword, asciihword, hword_asciipart, word, hword, hword_part
              WITH unaccent, simple
            """
        ]

        for query in queries {
            _ = try executeQueryString(query)
        }
    }

    /// Performs full-text search ignoring diacritics
    /// - Parameters:
    ///   - table: The table name to search in
    ///   - column: The column name to search
    ///   - searchText: The text to search for
    ///   - configName: The text search configuration to use
    /// - Returns: Query results
    @discardableResult open func fullTextSearchIgnoringDiacritics(
        table: String,
        column: String,
        searchText: String,
        configName: String = "simple_unaccent"
    ) throws -> [[String : Any]]? {
        let escapedSearchText = searchText.replacingOccurrences(of: "'", with: "''")

        let query = """
            SELECT * FROM \(table)
            WHERE to_tsvector('\(configName)', \(column)) @@ to_tsquery('\(configName)', '\(escapedSearchText)')
            """

        return try executeQueryString(query)
    }

}

