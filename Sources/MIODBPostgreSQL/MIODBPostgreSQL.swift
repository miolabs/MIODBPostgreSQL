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
import PostgresClientKit

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

extension PostgresError: LocalizedError {
    public var errorDescription: String? {
        switch self {
            case .cleartextPasswordCredentialRequired:
                return "[PostgresError] The Postgres server requires a `Credential.cleartextPassword` for authentication."
            case .connectionClosed:
                return "[PostgresError] An attempt was made to operate on a closed connection."
            case .connectionPoolClosed:
                return "[PostgresError] An attempt was made to operate on a closed connection pool."
            case .cursorClosed:
                return "[PostgresError] An attempt was made to operate on a closed cursor."
            case let .invalidParameterValue(name, value, allowedValues):
                return "[BMWSHelperError] The Postgres server has a parameter set to a value incompatible with PostgresClientKit. \n parameter: \(name) \n value: \(value) \n allowed values: \(allowedValues)"
            case .invalidUsernameString:
                return "[PostgresError] The specified username does not meet the SCRAM-SHA-256 requirements for a username."
            case .invalidPasswordString:
                return "[PostgresError] The specified password does not meet the SCRAM-SHA-256 requirements for a password."
            case .md5PasswordCredentialRequired:
                return "[PostgresError] The Postgres server requires a `Credential.md5Password` for authentication."
            case .scramSHA256CredentialRequired:
                return "[PostgresError] The Postgres server requires a `Credential.scramSHA256` for authentication."
            case let .serverError(description):
                return "[PostgresError] The Postgres server reported an internal error or returned an invalid response. \"\(description)\""
            case let .socketError(cause):
                return "[PostgresError] A network error occurred in communicating with the Postgres server. \"\(cause)\""
            case let .sqlError(notice):
                return "[PostgresError] The Postgres server reported a SQL error. \"\(notice)\""
            case let .sslError(cause):
                return "[PostgresError] An error occurred in establishing SSL/TLS encryption. \"\(cause)\""
            case .sslNotSupported:
                return "[PostgresError] The Postgres server does not support SSL/TLS."
            case .statementClosed:
                return "[PostgresError] An attempt was made to operate on a closed statement."
            case .timedOutAcquiringConnection:
                return "[PostgresError] The request for a connection failed because a connection was not allocated before the request timed out. (SeeAlso: `ConnectionPoolConfiguration.pendingRequestTimeout)"
            case .tooManyRequestsForConnections:
                return "[PostgresError] The request for a connection failed because the request backlog was too large. (SeeAlso: `ConnectionPoolConfiguration.maximumPendingRequests)"
            case .trustCredentialRequired:
                return "[PostgresError] The Postgres server requires a `Credential.trust` for authentication."
            case let .unsupportedAuthenticationType(authenticationType):
                return "[PostgresError] The authentication type \"\(authenticationType)\" required by the Postgres server is not supported by PostgresClientKit."
            case let .valueConversionError(value, type):
                return "[PostgresError] The value could not be converted to the requested type. \n value: \(value) \n type: \(type)"
            case .valueIsNil:
                return "[PostgresError] The value is `nil`."
        }
    }
}

open class MIODBPostgreSQL: MIODB {

    let defaultPort:Int32 = 5432
    let defaultUser = "root"
    let defaultDatabase = "public"
    let serverTimeZone = TimeZone(secondsFromGMT: 0)!
    
    var connection:PostgresClientKit.Connection?
        
    open override func connect() throws {
        if port == nil { port = defaultPort }
        if user == nil { user = defaultUser }
        if database == nil { database = defaultDatabase }
        
        var configuration = PostgresClientKit.ConnectionConfiguration()
        configuration.host = host!
        configuration.port = Int(port!)
        configuration.database = database!
        configuration.user = user!
        configuration.credential = .md5Password(password: password!)
        
        do {
            connection = try PostgresClientKit.Connection(configuration: configuration)
        }
        catch let error {
            throw MIODBPostgreSQLError.fatalError("Could not connect to POSTGRESQL Database. ERROR: \(error.localizedDescription)")
        }
    }
    
//    open func connect(scheme:String?) throws {
//        try connect()
//        try changeScheme(scheme)
//    }
    
    open override func disconnect() {
        connection?.close()
        connection = nil
    }
    
    @discardableResult open override func executeQueryString(_ query:String) throws -> [[String : Any]]? {
        queryWillExecute() // To notify the pool idle time out wher about to start
        
        defer {
            queryDidExecute() // To notify the pool idle time out we don't nned the conneciton anymore
        }
        
        var items:[[String : Any]]?
        try MIOCoreAutoReleasePool {
            items = try dictsFromQuery(query)
        }
        
        return items
    }
    
    func dictsFromQuery(_ query : String) throws  -> [[String : Any]] {
        
        do {
            guard let conn = connection else {
                throw MIODBPostgreSQLError.fatalError("Can't query a nil Connection.")
            }
            let statement = try conn.prepareStatement(text: query)
            defer { statement.close() }
            
            let cursor = try statement.execute(parameterValues: [], retrieveColumnMetadata: true)
            defer { cursor.close() }
            
            return try buildDictFromCursor(cursor)
        }
        catch let error as PostgresError {
            throw MIODBPostgreSQLError.fatalError("Query error: \(error.localizedDescription)")
        }
    }
    
    private func buildDictFromCursor(_ cursor : Cursor) throws -> [[String : Any]] {
        
        var dicts = [[String : Any]]()
        let numberOfCols = cursor.columns?.count ?? 0
        for row in cursor {
            var dict = [String : Any]()
            let columns = try row.get().columns
            for colIndex in 0..<numberOfCols {
                if let columnMetadata = cursor.columns?[colIndex] {
                    let key = columnMetadata.name
                    let valueType = columnMetadata.dataTypeOID
                    do {
                        dict[key] = try convert(value: columns[colIndex], withType: valueType)
                    }
                    catch let error as PostgresError {
                        switch error {
                            case .valueIsNil:
                                dict[key] = NSNull()
                                break
                            default:
                                throw error
                        }
                    }
                } else { // this shouldn't happen
                    print("[WARNING] Failed to retrieve column metadata");
                }
            }
            //let city = try columns[0].string()
            dicts.append(dict)
        }
        return dicts
    }
    
    func convert ( value:PostgresValue, withType type:UInt32 ) throws -> Any {
        
        var ret:Any? = nil
        try MIOCoreAutoReleasePool {
            switch type {

                case 16: ret = try value.bool() // Bool
                
                case 18: ret = MIOCoreInt8Value( try value.optionalInt() ) // UInt8, char
                case 20: ret = MIOCoreInt64Value( try value.optionalInt() ) // Int8
                case 21: ret = MIOCoreInt16Value( try value.optionalInt() ) // Int2
                case 23: ret = MIOCoreInt32Value( try value.optionalInt() ) // Int4
                
                case 1700, 700, 701, 790: // numeric, float4, float8, money
                    ret = try value.optionalDecimal()
                
                case 1114: ret = try value.optionalTimestamp()?.date(in: serverTimeZone) // Timestamp
                case 1184: ret = try value.optionalTimestamp()?.date(in: serverTimeZone) // Timestamp Z
                case 1082: ret = try value.optionalDate()?.date(in: serverTimeZone) // Date
                case 1083: ret = try value.optionalTime()?.date(in: serverTimeZone) // Time

                case 1043: // varchar
                    ret = try value.optionalString()
                    
                case 114, 3802: // json, jsonb (= transformable for us)
                    if let str = try value.optionalString() {
                        ret = try JSONSerialization.jsonObject(with: str.data(using: .utf8)!, options: [.allowFragments] )
                    }
                case 3807: // json binary array
                    if let str = try value.optionalString() {
                        ret = try JSONSerialization.jsonObject(with: str.data(using: .utf8)!, options: [.allowFragments] )
                    }

                case 2950: // UUID
                    if let str = try value.optionalString() {
                        ret = UUID( uuidString: str )
                    }
                    
                case 25,19: // Text, Name(used when getting information from the DB as which contraints/indices/etc has)
                    ret = try value.optionalString()
                    
                default:
                    NSLog("Type not implemented. Fallback to string. type: \(type)")
                    ret = try value.optionalString()
            }
            if ret == nil {
                ret = NSNull()
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
