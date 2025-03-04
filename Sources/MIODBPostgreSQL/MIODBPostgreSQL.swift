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
            return "[[MIODBPostgreSQLError] Fatal error. Code: \(code) Message: \"\(msg)\"."
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

    deinit { disconnect() }
    
    open override func connect( _ to_db: String? = nil ) throws {
        if port == nil { port = defaultPort }
        if user == nil { user = defaultUser }
        
        let final_db = to_db ?? database ?? defaultDatabase
        // if database == nil { database = defaultDatabase }
        
        //let connectionString = "host = \(host!) port = \(port!) user = \(user!) password = \(password!) dbname = \(database!) gssencmode='disable'"
        Log.trace( "Connecting to POSTGRESQL Database. Connection string: \(host!):\(port!)/\(final_db)")
        connectionString = "host = \(host!) port = \(port!) user = \(user!) password = \(password!) dbname = \(final_db)"
        _connection_str = connectionString!.cString(using: .utf8)
        
        _connection = PQconnectdb( _connection_str )
        let status = PQstatus( _connection )
        if  status != CONNECTION_OK {
            _connection = nil
//            throw MIODBPostgreSQLError.fatalError("-1", "Could not connect to POSTGRESQL Database. Connection string: \(connectionString!)")
            Log.critical( "Could not connect to POSTGRESQL Database. host:\(host!), port: \(port!), dbname:\(final_db)")
            throw MIODBPostgreSQLError.fatalError("-1", "Could not connect to POSTGRESQL Database.")
        }
        
        try super.connect(to_db)
    }
    
//    open func connect(scheme:String?) throws {
//        try connect()
//        try changeScheme(scheme)
//    }
    
    open override func disconnect() {
        if _connection != nil {
            PQfinish( _connection )
            _connection = nil
            _connection_str = nil
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
            Log.error( "[FATAL ERROR]: Postgres connection was lost, re-connecting and crossing fingers" )
            disconnect()
            usleep( 500000 ) // 0.5 seconds
            try connect()
        }
        
        Log.debug( "\(query)" )
        
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
                Log.debug( "MIODBPostgreSQL Error: \(errorMessage)" )
                throw MIODBPostgreSQLError.fatalError(code, errorMessage)
                
            case PGRES_EMPTY_QUERY    : Log.warning("Empty query")
            case PGRES_COPY_OUT       : Log.warning("Copy out")
            case PGRES_COPY_IN        : Log.warning("Copy in")
            case PGRES_BAD_RESPONSE   : Log.warning("Bad response")
            case PGRES_NONFATAL_ERROR : Log.warning("Non fatal error")
            case PGRES_COPY_BOTH      : Log.warning("Copy both")
            case PGRES_SINGLE_TUPLE   : Log.warning("Single tupple")
                
            default: 
                Log.warning("Response not implemented.")
            }
                                    
            return items
        }
                
        Log.trace( "\(r)" )
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
            Log.warning( "Type not implemented. Fallback to string. type: \(type)" )
            ret = str
        }
        
        return ret!
    }
    
    open override func changeScheme(_ scheme:String?) throws {
        if scheme == nil { return }
        
        if _connection == nil {
            throw MIODBPostgreSQLError.fatalError("-2","Could not change the scheme. The connection is nil")
        }
                
        try executeQueryString("SET search_path TO \(scheme!), public")
        try super.changeScheme(scheme)
    }
    
}

extension MDBQuery
{
//    public func encryptedField(_ field:String, value:String, salt:String) -> MDBQuery {
//        let v = "crypt('\(salt)', \(value))"
//        return field(field, value: v)
//    }
    
//    public func encryptedEqual(field:String, value:String, salt:String) -> MDBQuery {
//        let v = "crypt('\(salt)', \(value))"
//        return equal(field: field, value: v)
//    }
    
}
