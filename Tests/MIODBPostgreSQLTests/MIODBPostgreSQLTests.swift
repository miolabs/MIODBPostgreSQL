import XCTest
@testable import MIODBPostgreSQL


final class MIODBPostgreSQLTests: XCTestCase {
    let SkipTestSuite = true

    // This is call from LinuxMain.swift
    static var allTests = [
        ("testConnection", testConnection),
    ]

    func testConnection() throws {
        try XCTSkipIf(SkipTestSuite, "Life Test Suite not enabled")
        let host_key = ProcessInfo.processInfo.environment["HOST"] ?? ""
        let user_key = ProcessInfo.processInfo.environment["USER"] ?? ""
        let pass_key = ProcessInfo.processInfo.environment["PASS"] ?? ""
        let db_key   = ProcessInfo.processInfo.environment["DB"] ?? ""
        let sch_key   = ProcessInfo.processInfo.environment["SCHEMA"] ?? ""
        
        let db = MIODBPostgreSQL(host: host_key, user: user_key, password: pass_key, database: db_key, scheme: "_" + sch_key.replacing("-", with: "") )
        var found_key = false
        do {
            if let rows = try db.executeQueryString( "select * from server_status" ) {
                for r in rows {
                    if let key = r["key"] as? String {
                        if key == "sync_files" { found_key = true }
                        print( "# \( key )\t\( r["server_id"]! )\t\( r["value"]! )\t\( r["processing"]! )")
                    }
                }
            }
        }
        catch {
            print( "\(error.localizedDescription )" )
        }
        
        XCTAssert( found_key )
    }
   
}
