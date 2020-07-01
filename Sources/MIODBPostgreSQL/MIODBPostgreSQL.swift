//
//  MIODBPostgresSQL.swift
//  MIODB
//
//  Created by Javier Segura Perez on 24/12/2019.
//  Copyright © 2019 Javier Segura Perez. All rights reserved.
//

import Foundation
import MIODB
import CLibPQ

enum MIODBPostgreSQLError: Error {
    case fatalError(_ msg: String)
}

extension MIODBPostgreSQLError: LocalizedError {
    public var errorDescription: String? {
        switch self {
        case let .fatalError(msg):
            return "[MIODBPostgreSQLError] Fatal error \"\(msg)\"."
        }
    }
}

open class MIODBPostgreSQL: MIODB {

    public var scheme:String?
    
    let defaultPort:Int32 = 5432
    let defaultUser = "root"
    let defaultDatabase = "public"
    
    var connection:OpaquePointer?
    
//    public init(host:String, port:Int32?, user:String?, password:String?, database:String?, schema:String?){
//        self.host = host
//        self.port = port
//        self.user = user
//        self.password = password
//        self.database = database
//        self.schema = schema
//    }
    
    open override func connect(){
        if port == nil { port = defaultPort }
        if user == nil { user = defaultUser }
        if database == nil { database = defaultDatabase }
        
        connection = PQconnectdb("host = \(host!) port = \(port!) user = \(user!) password = \(password!) dbname = \(database!) gssencmode='disable'".cString(using: .utf8))
        let status = PQstatus(connection)
        if  status == CONNECTION_OK {
            changeScheme(scheme)
        }
        else {
            connection = nil
        }
    }
    
    open func connect(scheme:String?){
        connect()
        changeScheme(scheme)
    }
    
    open override func disconnect() {
        PQfinish(connection)
        connection = nil
    }
    
    open override func executeQueryString(_ query:String) throws -> [Any]{
        
        if isInsideTransaction {
            pushQueryString(query)
            return []
        }
        
        let res = PQexec(connection, query.cString(using: .utf8))
        defer {
            PQclear(res)
        }
        
        var items:[Any] = []
        
        switch PQresultStatus(res) {
                        
        case PGRES_EMPTY_QUERY:
            print("Empty query")
            
        case PGRES_COMMAND_OK:
            break
//            print("Command OK")
            
        case PGRES_TUPLES_OK:
            for row in 0..<PQntuples(res) {
                var item = [String:Any]()
                for col in 0..<PQnfields(res){
                    if PQgetisnull(res, row, col) == 1 {continue}
                    
                    let colname = String(cString: PQfname(res, col))
                    let type = PQftype(res, col)
                    let value = PQgetvalue(res, row, col)
                    
                    item[colname] = convert(value: value!, withType: type)
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
            throw MIODBPostgreSQLError.fatalError(String(cString: PQresultErrorMessage(res)) + "\n" + query)
            
        case PGRES_COPY_BOTH:
            print("Copy both")
            
        case PGRES_SINGLE_TUPLE:
            print("Single tupple")
        
        default:
            print("Response not implemented." +  String(cString: PQresultErrorMessage(res)))
        
        }
                            
        return items
    }
    
    func convert(value:UnsafePointer<Int8>, withType type:Oid) -> Any {
                        
        
        switch type {
        case 16: // Boolean
            return value[0] == 116 ? true : false

        case 20: // Int8
            return Int64(String(cString: value))!
            
        case 23: // Int 4
            return Int32(String(cString: value))!
            
        case 21: // Int 16
            return Int16(String(cString: value))!
            
        case 18: // UInt8, char
            return Int8(String(cString: value))!
            
        case 1700, 701: // numeric, float8
            return Decimal(string: String(cString: value))!
            
        case 1114: // Timestamp
            return String(cString: value) // return dates as strings
        case 1043: // varchar
            return String(cString: value)
        case 114: // json
            return String(cString: value)
        default:
            return String(cString: value)
        }
    }
    
    open func changeScheme(_ scheme:String?){
        if let scheme = scheme {
            _ = try! executeQueryString("SET search_path TO \(scheme)")
            self.scheme = scheme
        }
    }
    
}

extension MDBQuery
{
    public func encryptedField(_ field:String, value:String, salt:String) -> MDBQuery {
        let v = "crypt('\(salt)', \(value))"
        return field(field, value: v)
    }
    
    public func encryptedEqual(field:String, value:String, salt:String) -> MDBQuery {
        let v = "crypt('\(salt)', \(value))"
        return equal(field: field, value: v)
    }
}
