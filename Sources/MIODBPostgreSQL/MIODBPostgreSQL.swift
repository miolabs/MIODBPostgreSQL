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

enum MIODBPostgreSQLError: Error {
    case fatalError(_ msg: String)
}

extension MIODBPostgreSQLError: LocalizedError {
    public var errorDescription: String? {
        switch self {
        case let .fatalError(msg):
            return "[[MIODBPostgreSQLError] Fatal error \"\(msg)\"."
        }
    }
}

open class MIODBPostgreSQL: MIODB {

    let defaultPort:Int32 = 5432
    let defaultUser = "root"
    let defaultDatabase = "public"
    
    var connection:OpaquePointer?
        
    open override func connect( _ to_db: String? = nil ) throws {
        if port == nil { port = defaultPort }
        if user == nil { user = defaultUser }
        
        let final_db = to_db ?? database ?? defaultDatabase
        // if database == nil { database = defaultDatabase }
        
        //let connectionString = "host = \(host!) port = \(port!) user = \(user!) password = \(password!) dbname = \(database!) gssencmode='disable'"
        connectionString = "host = \(host!) port = \(port!) user = \(user!) password = \(password!) dbname = \(final_db)"
        connection = PQconnectdb(connectionString!.cString(using: .utf8))
        let status = PQstatus(connection)
        if  status != CONNECTION_OK {
            connection = nil
            throw MIODBPostgreSQLError.fatalError("Could not connect to POSTGRESQL Database. Connection string: \(connectionString!)")
        }
    }
    
//    open func connect(scheme:String?) throws {
//        try connect()
//        try changeScheme(scheme)
//    }
    
    open override func disconnect() {
        PQfinish(connection)
        connection = nil
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
        
//        if isInsideTransaction {
//            pushQueryString(query)
//            return []
//        }
        
        if ( PQstatus(connection) != CONNECTION_OK ) {
            print( "[[FATAL ERROR]: Postgres connection was lost, re-connecting and crossing fingers")
            disconnect()
            usleep( 500000 ) // 0.5 seconds
            try connect()
        }


        let res = PQexec(connection, Array(query.utf8CString))
        
        defer {
            PQclear(res)
        }
        
        var items:[[String : Any]] = []
        
        switch PQresultStatus(res) {
                        
        case PGRES_EMPTY_QUERY:
            print("Empty query")
            
        case PGRES_COMMAND_OK:
            break
//            print("Command OK")
            
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
            
        case PGRES_COPY_OUT:
            print("Copy out")
            
        case PGRES_COPY_IN:
            print("Copy in")
            
        case PGRES_BAD_RESPONSE:
            print("Bad response")	
            
        case PGRES_NONFATAL_ERROR:
            print("Non fatal error")
              
        case PGRES_FATAL_ERROR:
            let errorMessage = (scheme != nil ? "\(scheme!): " : "") + String(cString: PQresultErrorMessage(res)) + "\n" + query
            throw MIODBPostgreSQLError.fatalError(errorMessage)
            
        case PGRES_COPY_BOTH:
            print("Copy both")
            
        case PGRES_SINGLE_TUPLE:
            print("Single tupple")
        
        default:
            print("Response not implemented." +  String(cString: PQresultErrorMessage(res)))
        
        }
                            
        return items
    }
    
    func convert ( value:UnsafePointer<Int8>, withType type:Oid ) throws -> Any {
        if type == 16 { // Boolean
            return (value[0] == 116) as Bool
        }

        
        var ret:Any?


        try MIOCoreAutoReleasePool {
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
                
            default:
                NSLog("Type not implemented. Fallback to string. type: \(type)")
                ret = str
            }
        }
        
        return ret!
    }
    
    open override func changeScheme(_ scheme:String?) throws {
        if connection == nil {
            throw MIODBPostgreSQLError.fatalError("Could not change the scheme. The connection is nil")
        }
        
        if scheme != nil {
            try executeQueryString("SET search_path TO \(scheme!), public")
            self.scheme = scheme!
        }
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
