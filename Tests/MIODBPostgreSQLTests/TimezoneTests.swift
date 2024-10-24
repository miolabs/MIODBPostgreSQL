import XCTest
@testable import MIODB
@testable import MIODBPostgreSQL
import CLibPQ

// --------------------------------------------
// Timezone assessments for miodb & postgreSQL
// --------------------------------------------
//
// - Warning: all these tests were run in european summer time. The results may vary if run in winter time
//
// --------- Timezone behaviour V2 as a result of the findings --------
//
// - We will be able to use timezone information in the queries. The MDBTZ class has deen developped for this purpose. Any date has to be enclosed in
//    MDBTZ() when used in a query involving a field of type 'timestamp with timezone'
// - When reading from database a field of type 'timestamp with timezone' we will use the timezone information provided by postgreSQL. This behaviour
//    can be reversed with the static variable timeZoneBehaviourV2 set to false in MIODBPostgreSQL. 
//
//
// --------- Findings (MIODB timestamps processing) ---------------
//
// - We loose the milliseconds when reading from database 
// - All datetime info is written in its local version. For example, 16:10:00 gmt is converted in 18:10:00 when saving in the database in Spain timezone
//    because MDBValue formats the dates that way, using the locale and loosing any timezone information before it has any chance to reach the database
// - This is done regardless of the type of column in database, so the "timestamp with timezone" dont get a correct value (it is interpreted as being 
//    in gmt). There is no point in using "timestamp with timezone" columns in the current (sept 2024) MIODB implementation
// - We are always getting the same value reading from a "timestamp with timezone" column or a "timestamp without timezone" column
// - The values are interpreted as local timezone when they are read. So:
//      - No absolute value set in one timezone is preserved when read. For example, 18:10:00 in Dubai is read as 18:10:00 in Spain, which is a 
//         different moment in time. This is however the expected behaviour and it is desired (to get reports about what happens in the system at, for
//         example, 10 am, regardless of the timezone of the data) 
//      - All values read from the database in the same timezone where they were written are correct
// - Processing the values received from PostgreSQL:
//      - Fields 'timestamp' are postgre type 1114. They got through MIOCoreDate() to be converted from string (2024-10-17 16:10:17) to Date(). 
//          Then the dataformatter  _mcd_date_time_formatter_s is used. It does not specify any timezone and the date is interpreted as being 
//          in local time
//      - Fields 'timestamp with timezone' are postgre type 1184. They got through MIOCoreDate() to be converted from string (2024-10-17 16:10:17+00)
//          to Date(). The trailing +00 is removed, so we would loose here any information about timezone, should it be there. 
//          Then the same dataformatter is applied 
//
//



//
// Environment setup with docker
// ------------------------------
// 1.- Get the postgres image: 
//   docker pull postgres:16.3
// 2.- Create a container with the following command:
//   docker run --name Test01 -p 5432:5432 -e POSTGRES_USER=user -e POSTGRES_DB=DBTest01 -e POSTGRES_PASSWORD=pass -e DATABASE_HOST=localhost -d postgres:16.3
//
//  This test is cumbersome. You need to run it from a machine with timezone set to Europe/Madrid, then to Asia/Dubai for the initial setup.
//  Then you have to do it again for the full results
//
// Check out the values of the following MIODBPostgreSQLTests class properties:
// - SkipTestSuite variable is true
// - host, user, pass, db match the values passed to the container
//
// Manage the database
// ------------------------------
// - Access: 
//      psql -U user -d DBTest01
// - Delete all schemas:
//      DO $$ DECLARE schema_name text; BEGIN FOR schema_name IN SELECT nspname FROM pg_namespace WHERE nspname LIKE '_000000%' LOOP EXECUTE format('DROP SCHEMA IF EXISTS %I CASCADE', schema_name); END LOOP;END $$;
// - Delete all tables from public:
//      DO $$ DECLARE tabname text; BEGIN FOR tabname IN SELECT tablename FROM pg_tables WHERE schemaname = 'public' LOOP EXECUTE 'DROP TABLE IF EXISTS ' || quote_ident(tabname) || ' CASCADE'; END LOOP; END $$;

final class TimezoneTests: XCTestCase {
    //// Set to true to skip the test suite 
    private let SkipTestSuite = false  // We could use defines at project level instead of this
    //// MongoDB connection data
    private static let host: String  = "localhost"
    private static let user: String  = "user"
    private static let pass: String  = "pass"
    private static let db01: String  = "DBTest01"
    private static let port01: Int32 = 5432

    private static var conn01 : MDBPostgreConnection? = nil
    private static var inst01 : MIODBPostgreSQL? = nil

    private let schemaTimezones = "timezones"
    private let uuidNoMDBTZ = "00000000-0000-0000-0000-000000000001" // all fields are inserted as Date()
    private let uuidMDBTZ   = "00000000-0000-0000-0000-000000000002" // Timezoned columns are inserted as MDBTZ()

    override class func setUp() {
        conn01 = MDBPostgreConnection(host: TimezoneTests.host, port: TimezoneTests.port01, user: TimezoneTests.user, password: TimezoneTests.pass)
        do {
            inst01 = try conn01?.create(db01) as? MIODBPostgreSQL
            //try TimezoneTests.inst!.dropCollection(TimezoneTests.testCollection)

        } catch {
            XCTFail("Error setting up test suite: cant connect to postgreSQL database")
        }
        super.setUp()
    }

    override class func tearDown() {
        TimezoneTests.inst01?.disconnect()
        super.tearDown()
    }

// MARK: - utils schema    

    func setupTimezoneTables() throws {
        let tablaDubai = """
                CREATE TABLE esquema.dubai (
                    id UUID PRIMARY KEY,
                    date_ntz_sp TIMESTAMP,
                    date_ntz_ea TIMESTAMP WITHOUT TIME ZONE,
                    date_ntz_ny TIMESTAMP,
                    date_ntz_zz TIMESTAMP,
                    date_tz_sp TIMESTAMP WITH TIME ZONE,
                    date_tz_ea TIMESTAMP WITH TIME ZONE,
                    date_tz_ny TIMESTAMP WITH TIME ZONE,
                    date_tz_zz TIMESTAMP WITH TIME ZONE,
                    date_creation_ntz TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    date_creation_tz TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
                    );
                """
        let tablaSpain = """
                CREATE TABLE esquema.spain (
                    id UUID PRIMARY KEY,
                    date_ntz_sp TIMESTAMP,
                    date_ntz_ea TIMESTAMP WITHOUT TIME ZONE,
                    date_ntz_ny TIMESTAMP,
                    date_ntz_zz TIMESTAMP,
                    date_tz_sp TIMESTAMP WITH TIME ZONE,
                    date_tz_ea TIMESTAMP WITH TIME ZONE,
                    date_tz_ny TIMESTAMP WITH TIME ZONE,
                    date_tz_zz TIMESTAMP WITH TIME ZONE,
                    date_creation_ntz TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    date_creation_tz TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
                    );
                """
        try TimezoneTests.inst01?.executeQueryString(tablaDubai.replacingOccurrences(of: "esquema", with: schemaTimezones))
        try TimezoneTests.inst01?.executeQueryString(tablaSpain.replacingOccurrences(of: "esquema", with: schemaTimezones))
    }

    func setupTimezoneTestsTables() throws {
        try TimezoneTests.inst01?.executeQueryString("DROP SCHEMA IF EXISTS \(schemaTimezones) CASCADE")
        try TimezoneTests.inst01?.executeQueryString("CREATE SCHEMA \(schemaTimezones)")
        try setupTimezoneTables()
    }

    func existeTimezoneTables() throws -> Bool{
        let query = """
            SELECT EXISTS (
                SELECT 1
                FROM information_schema.schemata
                WHERE schema_name = '\(schemaTimezones)'
            );
            """
        let resp = try TimezoneTests.inst01?.executeQueryString(query)
        //print("Existe tabla dubai: \(resp![0]["exists"] as! Bool)")
        return resp![0]["exists"] as! Bool
    }
   
// MARK: - utils datetime    
    var _mcd_date_time_formatter_t_s:DateFormatter?
    func mcd_date_time_formatter_t_s() -> DateFormatter {
        if _mcd_date_time_formatter_t_s == nil {
            let df = DateFormatter()
            df.locale = Locale.current
            df.dateFormat = "yyyy-MM-dd'T'HH:mm:ss"
            _mcd_date_time_formatter_t_s = df
        }
        return _mcd_date_time_formatter_t_s!
    }

    func newMIOCoreDateTime(_ date:String) -> Date {
        let formatter = mcd_date_time_formatter_t_s()
        if let dateTime = formatter.date(from: date) {
            return dateTime
        }
        XCTFail("Error creating datetime. Expected format: yyyy-MM-dd HH:mm:ss")
        return Date()
    }

    func newDateTime(_ date:String, _ secondsFromGMT: Int) -> Date {
        let formatter = DateFormatter()
        formatter.dateFormat = "yyyy-MM-dd HH:mm:ss"
        formatter.timeZone = TimeZone(secondsFromGMT: secondsFromGMT)
        if let dateTime = formatter.date(from: date) {
            return dateTime
        }
        XCTFail("Error creating datetime. Expected format: yyyy-MM-dd HH:mm:ss")
        return Date()
    }

    // Just to get better grip about how the formatters work
    func testDatetime_Formatters() throws{
        let timeZoneSpain = TimeZone(identifier : "Europe/Madrid")
        let timeZoneMIOCore = Locale.current.timeZone
        let date_utc = newDateTime("2024-10-17 16:10:17", 0)
        let date_sp  = newDateTime("2024-10-17 16:10:17", timeZoneSpain!.secondsFromGMT())
        let miodate  = newMIOCoreDateTime("2024-10-17T16:10:17")
        XCTAssertEqual(timeZoneSpain!.identifier, "Europe/Madrid")
        XCTAssertEqual(date_utc.description, "2024-10-17 16:10:17 +0000")
        XCTAssertEqual(date_sp.description, "2024-10-17 14:10:17 +0000")
        if timeZoneSpain!.identifier == timeZoneMIOCore?.identifier {
            XCTAssertEqual(miodate.description, "2024-10-17 14:10:17 +0000")
        }
    }
       
// MARK: - No MDBTZ spain

    func checkInserts_No_TZ_FromSpain() throws {
        let date_zz = newDateTime("2024-10-17 16:10:17", 0)       
        let date_ea = newDateTime("2024-10-17 16:10:17", 14400)   
        let date_sp = newDateTime("2024-10-17 16:10:17", 7200)
        let date_ny = newDateTime("2024-10-17 16:10:17", -21600)

        XCTAssertEqual(date_zz, newDateTime("2024-10-17 18:10:17", 7200))
        XCTAssertEqual(date_ea, newDateTime("2024-10-17 14:10:17", 7200))   
        XCTAssertEqual(date_sp, newDateTime("2024-10-17 16:10:17", 7200))
        XCTAssertEqual(date_ny, newDateTime("2024-10-18 00:10:17", 7200))

        let query1 = try MDBQuery( schemaTimezones + ".spain") { 
                Insert( [
                    "id": uuidNoMDBTZ,
                    "date_ntz_zz": date_zz,
                    "date_ntz_ea": date_ea,
                    "date_ntz_ny": date_ny,
                    "date_ntz_sp": date_sp,
                    "date_tz_zz": date_zz,
                    "date_tz_ea": date_ea,
                    "date_tz_ny": date_ny,
                    "date_tz_sp": date_sp,
                ] )
            }
        XCTAssertEqual(query1.values["date_ntz_zz"]?.value, "'2024-10-17T18:10:17'")
        XCTAssertEqual(query1.values["date_tz_zz" ]?.value, "'2024-10-17T18:10:17'")
        XCTAssertEqual(query1.values["date_ntz_ea"]?.value, "'2024-10-17T14:10:17'")
        XCTAssertEqual(query1.values["date_tz_ea" ]?.value, "'2024-10-17T14:10:17'")
        XCTAssertEqual(query1.values["date_ntz_sp"]?.value, "'2024-10-17T16:10:17'")
        XCTAssertEqual(query1.values["date_tz_sp" ]?.value, "'2024-10-17T16:10:17'")
        XCTAssertEqual(query1.values["date_ntz_ny"]?.value, "'2024-10-18T00:10:17'")
        XCTAssertEqual(query1.values["date_tz_ny" ]?.value, "'2024-10-18T00:10:17'")

        let data = try TimezoneTests.inst01?.executeQueryString(MDBQueryEncoderSQL(
                                                    MDBQuery(schemaTimezones + ".spain").select().where().addCondition("id", .equal, uuidNoMDBTZ)
                                                    ).rawQuery())
        if data!.count == 0 {  // no data yet. Insert
            // dubai: 2024-10-17 20:10:17 | 2024-10-17 16:10:17 | 2024-10-17 18:10:17+00
            try TimezoneTests.inst01?.executeQueryString(MDBQueryEncoderSQL(query1).rawQuery())
        }  
    }

    func checkReads_No_TZ_OfSpainFromSpain_Fields_NTZ() throws {
        let date_zz = newDateTime("2024-10-17 16:10:17", 0)
        let date_ea = newDateTime("2024-10-17 16:10:17", 14400) 
        let date_sp = newDateTime("2024-10-17 16:10:17", 7200)
        let date_ny = newDateTime("2024-10-17 16:10:17", -21600)

        let resp = try TimezoneTests.inst01?.executeQueryString(MDBQueryEncoderSQL(
                                                    MDBQuery(schemaTimezones + ".spain").select().where().addCondition("id", .equal, uuidNoMDBTZ)
                                                    ).rawQuery())
        XCTAssertEqual(resp!.count, 1)
        XCTAssertEqual(resp![0]["date_ntz_zz"] as! Date, date_zz)
        XCTAssertEqual(resp![0]["date_ntz_sp"] as! Date, date_sp)
        XCTAssertEqual(resp![0]["date_ntz_ea"] as! Date, date_ea)
        XCTAssertEqual(resp![0]["date_ntz_ny"] as! Date, date_ny)
        XCTAssertEqual((resp![0]["date_ntz_zz"] as! Date).description, "2024-10-17 16:10:17 +0000")
        XCTAssertEqual((resp![0]["date_ntz_sp"] as! Date).description, "2024-10-17 14:10:17 +0000")
        XCTAssertEqual((resp![0]["date_ntz_ea"] as! Date).description, "2024-10-17 12:10:17 +0000")
        XCTAssertEqual((resp![0]["date_ntz_ny"] as! Date).description, "2024-10-17 22:10:17 +0000")
    }

    func checkReads_No_TZ_OfSpainFromSpain_Fields_TZ() throws {
        let date_zz = newDateTime("2024-10-17 16:10:17", 0)
        let date_ea = newDateTime("2024-10-17 16:10:17", 14400) 
        let date_sp = newDateTime("2024-10-17 16:10:17", 7200)
        let date_ny = newDateTime("2024-10-17 16:10:17", -21600)

        let resp = try TimezoneTests.inst01?.executeQueryString(MDBQueryEncoderSQL(
                                                    MDBQuery(schemaTimezones + ".spain").select().where().addCondition("id", .equal, uuidNoMDBTZ)
                                                    ).rawQuery())
        XCTAssertEqual(resp!.count, 1)
        if MIODBPostgreSQL.timeZoneBehaviourV2 {
            XCTAssertNotEqual(resp![0]["date_tz_zz"] as! Date, date_zz)
            XCTAssertNotEqual(resp![0]["date_tz_sp"] as! Date, date_sp)
            XCTAssertNotEqual(resp![0]["date_tz_ea"] as! Date, date_ea)
            XCTAssertNotEqual(resp![0]["date_tz_ny"] as! Date, date_ny)
            XCTAssertNotEqual((resp![0]["date_tz_zz"] as! Date).description, "2024-10-17 16:10:17 +0000")
            XCTAssertNotEqual((resp![0]["date_tz_sp"] as! Date).description, "2024-10-17 14:10:17 +0000")
            XCTAssertNotEqual((resp![0]["date_tz_ea"] as! Date).description, "2024-10-17 12:10:17 +0000")
            XCTAssertNotEqual((resp![0]["date_tz_ny"] as! Date).description, "2024-10-17 22:10:17 +0000")
        }
        else {
            XCTAssertEqual(resp![0]["date_tz_zz"] as! Date, date_zz)
            XCTAssertEqual(resp![0]["date_tz_sp"] as! Date, date_sp)
            XCTAssertEqual(resp![0]["date_tz_ea"] as! Date, date_ea)
            XCTAssertEqual(resp![0]["date_tz_ny"] as! Date, date_ny)
            XCTAssertEqual((resp![0]["date_tz_zz"] as! Date).description, "2024-10-17 16:10:17 +0000")
            XCTAssertEqual((resp![0]["date_tz_sp"] as! Date).description, "2024-10-17 14:10:17 +0000")
            XCTAssertEqual((resp![0]["date_tz_ea"] as! Date).description, "2024-10-17 12:10:17 +0000")
            XCTAssertEqual((resp![0]["date_tz_ny"] as! Date).description, "2024-10-17 22:10:17 +0000")
        }
    }

    func checkReads_No_TZ_OfDubaiFromSpain_NTZ() throws {
        let date_zz = newDateTime("2024-10-17 16:10:17", 0)
        let date_ea = newDateTime("2024-10-17 16:10:17", 14400) 
        let date_sp = newDateTime("2024-10-17 16:10:17", 7200)
        let date_ny = newDateTime("2024-10-17 16:10:17", -21600)

        //let dubaiGmtOffset = TimeZone(identifier: "Asia/Dubai")!.secondsFromGMT()
        let dubaiSpainOffset = TimeZone(identifier: "Asia/Dubai")!.secondsFromGMT() - TimeZone(identifier: "Europe/Madrid")!.secondsFromGMT()

        let resp = try TimezoneTests.inst01?.executeQueryString(MDBQueryEncoderSQL(
                                                    MDBQuery(schemaTimezones + ".dubai").select().where().addCondition("id", .equal, uuidNoMDBTZ)
                                                    ).rawQuery())  
        if resp?.count ?? 0 > 0 {
            print("checkTimezonesFromSpain: checking dubai values")
            XCTAssertEqual(resp!.count, 1)
            XCTAssertEqual(resp![0]["date_ntz_zz"] as! Date, date_zz.addingTimeInterval(TimeInterval(dubaiSpainOffset)))
            XCTAssertEqual(resp![0]["date_ntz_sp"] as! Date, date_sp.addingTimeInterval(TimeInterval(dubaiSpainOffset)))
            XCTAssertEqual(resp![0]["date_ntz_ea"] as! Date, date_ea.addingTimeInterval(TimeInterval(dubaiSpainOffset)))
            XCTAssertEqual(resp![0]["date_ntz_ny"] as! Date, date_ny.addingTimeInterval(TimeInterval(dubaiSpainOffset)))
            XCTAssertEqual((resp![0]["date_ntz_zz"] as! Date).description, "2024-10-17 18:10:17 +0000") // written to db: 2024-10-17T20:10:17
            XCTAssertEqual((resp![0]["date_ntz_sp"] as! Date).description, "2024-10-17 16:10:17 +0000") // written to db: 2024-10-17T18:10:17
            XCTAssertEqual((resp![0]["date_ntz_ea"] as! Date).description, "2024-10-17 14:10:17 +0000") // written to db: 2024-10-17T16:10:17
            XCTAssertEqual((resp![0]["date_ntz_ny"] as! Date).description, "2024-10-18 00:10:17 +0000") // written to db: 2024-10-18T02:10:17
        }
    }

    func checkReads_No_TZ_OfDubaiFromSpain_TZ() throws {
        let date_zz = newDateTime("2024-10-17 16:10:17", 0)
        let date_ea = newDateTime("2024-10-17 16:10:17", 14400) 
        let date_sp = newDateTime("2024-10-17 16:10:17", 7200)
        let date_ny = newDateTime("2024-10-17 16:10:17", -21600)

        let dubaiGmtOffset = TimeZone(identifier: "Asia/Dubai")!.secondsFromGMT()
        let dubaiSpainOffset = TimeZone(identifier: "Asia/Dubai")!.secondsFromGMT() - TimeZone(identifier: "Europe/Madrid")!.secondsFromGMT()

        let resp = try TimezoneTests.inst01?.executeQueryString(MDBQueryEncoderSQL(
                                                    MDBQuery(schemaTimezones + ".dubai").select().where().addCondition("id", .equal, uuidNoMDBTZ)
                                                    ).rawQuery())  
        if resp?.count ?? 0 > 0 {
            print("checkTimezonesFromSpain: checking dubai values")
             // This is what we should get (but we do not, hence the NotEqual)
            XCTAssertNotEqual(resp![0]["date_tz_zz"]  as! Date, date_zz)
            XCTAssertNotEqual(resp![0]["date_tz_sp"]  as! Date, date_sp)
            XCTAssertNotEqual(resp![0]["date_tz_ea"]  as! Date, date_ea)
            XCTAssertNotEqual(resp![0]["date_tz_ny"]  as! Date, date_ny)
            // This is what we really get when writing without MDBTZ to fields with timezone and then read from another timezone
            if MIODBPostgreSQL.timeZoneBehaviourV2 {
                XCTAssertEqual(resp![0]["date_tz_zz"]  as! Date, date_zz.addingTimeInterval(TimeInterval(dubaiGmtOffset)))
                XCTAssertEqual(resp![0]["date_tz_sp"]  as! Date, date_sp.addingTimeInterval(TimeInterval(dubaiGmtOffset)))
                XCTAssertEqual(resp![0]["date_tz_ea"]  as! Date, date_ea.addingTimeInterval(TimeInterval(dubaiGmtOffset)))
                XCTAssertEqual(resp![0]["date_tz_ny"]  as! Date, date_ny.addingTimeInterval(TimeInterval(dubaiGmtOffset)))
                XCTAssertEqual((resp![0]["date_tz_zz"]  as! Date).description, "2024-10-17 20:10:17 +0000") // written to db: 2024-10-17T20:10:17
                XCTAssertEqual((resp![0]["date_tz_sp"]  as! Date).description, "2024-10-17 18:10:17 +0000") // written to db: 2024-10-17T18:10:17
                XCTAssertEqual((resp![0]["date_tz_ea"]  as! Date).description, "2024-10-17 16:10:17 +0000") // written to db: 2024-10-17T16:10:17
                XCTAssertEqual((resp![0]["date_tz_ny"]  as! Date).description, "2024-10-18 02:10:17 +0000") // written to db: 2024-10-18T02:10:17
            }
            else {
                XCTAssertEqual(resp![0]["date_tz_zz"]  as! Date, date_zz.addingTimeInterval(TimeInterval(dubaiSpainOffset)))
                XCTAssertEqual(resp![0]["date_tz_sp"]  as! Date, date_sp.addingTimeInterval(TimeInterval(dubaiSpainOffset)))
                XCTAssertEqual(resp![0]["date_tz_ea"]  as! Date, date_ea.addingTimeInterval(TimeInterval(dubaiSpainOffset)))
                XCTAssertEqual(resp![0]["date_tz_ny"]  as! Date, date_ny.addingTimeInterval(TimeInterval(dubaiSpainOffset)))
                XCTAssertEqual((resp![0]["date_tz_zz"]  as! Date).description, "2024-10-17 18:10:17 +0000") // written to db: 2024-10-17T20:10:17
                XCTAssertEqual((resp![0]["date_tz_sp"]  as! Date).description, "2024-10-17 16:10:17 +0000") // written to db: 2024-10-17T18:10:17
                XCTAssertEqual((resp![0]["date_tz_ea"]  as! Date).description, "2024-10-17 14:10:17 +0000") // written to db: 2024-10-17T16:10:17
                XCTAssertEqual((resp![0]["date_tz_ny"]  as! Date).description, "2024-10-18 00:10:17 +0000") // written to db: 2024-10-18T02:10:17
            }
        }
    }

    func test_No_MDBTZ_Madrid() throws{
        try XCTSkipIf(SkipTestSuite, "TimezoneTests Suite not enabled")
        try XCTSkipIf(TimeZone.current.identifier != "Europe/Madrid", "test for Madrid timezone")
        let existenTablas = try existeTimezoneTables()
        if !existenTablas {
            try setupTimezoneTestsTables()
        }
        try checkInserts_No_TZ_FromSpain()
        try checkReads_No_TZ_OfSpainFromSpain_Fields_NTZ()
        try checkReads_No_TZ_OfSpainFromSpain_Fields_TZ()
        try checkReads_No_TZ_OfDubaiFromSpain_NTZ()
        try checkReads_No_TZ_OfDubaiFromSpain_TZ()
    }


// MARK: - No MDBTZ dubai
    func checkInserts_No_TZ_FromDubai() throws {
        let date_zz = newDateTime("2024-10-17 16:10:17", 0)
        let date_ea = newDateTime("2024-10-17 16:10:17", 14400) 
        let date_sp = newDateTime("2024-10-17 16:10:17", 7200)
        let date_ny = newDateTime("2024-10-17 16:10:17", -21600)
        let query1 = try MDBQuery( schemaTimezones + ".dubai") {
                Insert( [
                    "id": uuidNoMDBTZ,
                    "date_ntz_zz": date_zz,
                    "date_ntz_ea": date_ea,
                    "date_ntz_ny": date_ny,
                    "date_ntz_sp": date_sp,
                    "date_tz_zz": date_zz,
                    "date_tz_ea": date_ea,
                    "date_tz_ny": date_ny,
                    "date_tz_sp": date_sp,
                ] )
            }
        XCTAssertEqual(query1.values["date_ntz_zz"]?.value, "'2024-10-17T20:10:17'")
        XCTAssertEqual(query1.values["date_ntz_ea"]?.value, "'2024-10-17T16:10:17'")
        XCTAssertEqual(query1.values["date_ntz_sp"]?.value, "'2024-10-17T18:10:17'")
        XCTAssertEqual(query1.values["date_ntz_ny"]?.value, "'2024-10-18T02:10:17'")
        XCTAssertEqual(query1.values["date_tz_zz"]?.value, "'2024-10-17T20:10:17'")
        XCTAssertEqual(query1.values["date_tz_ea"]?.value, "'2024-10-17T16:10:17'")
        XCTAssertEqual(query1.values["date_tz_sp"]?.value, "'2024-10-17T18:10:17'")
        XCTAssertEqual(query1.values["date_tz_ny"]?.value, "'2024-10-18T02:10:17'")

        let data = try TimezoneTests.inst01?.executeQueryString(MDBQueryEncoderSQL(
                                                    MDBQuery(schemaTimezones + ".dubai").select().where().addCondition("id", .equal, uuidNoMDBTZ)
                                                    ).rawQuery())
        if data!.count == 0 {  // no data yet. Insert
            try TimezoneTests.inst01?.executeQueryString(MDBQueryEncoderSQL(query1).rawQuery())
        }       
    }

    func checkReads_No_TZ_OfDubaiFromDubai_Fields_NTZ() throws {
        let date_zz = newDateTime("2024-10-17 16:10:17", 0)
        let date_ea = newDateTime("2024-10-17 16:10:17", 14400) 
        let date_sp = newDateTime("2024-10-17 16:10:17", 7200)
        let date_ny = newDateTime("2024-10-17 16:10:17", -21600)
    
        let resp = try TimezoneTests.inst01?.executeQueryString(MDBQueryEncoderSQL(
                                                    MDBQuery(schemaTimezones + ".dubai").select().where().addCondition("id", .equal, uuidNoMDBTZ)
                                                    ).rawQuery())
        XCTAssertEqual(resp!.count, 1)
        XCTAssertEqual(resp![0]["date_ntz_zz"] as! Date, date_zz)
        XCTAssertEqual(resp![0]["date_ntz_sp"] as! Date, date_sp)
        XCTAssertEqual(resp![0]["date_ntz_ea"] as! Date, date_ea)
        XCTAssertEqual(resp![0]["date_ntz_ny"] as! Date, date_ny)
        XCTAssertEqual((resp![0]["date_ntz_zz"] as! Date).description, "2024-10-17 16:10:17 +0000") // written to db: 2024-10-17T20:10:17
        XCTAssertEqual((resp![0]["date_ntz_sp"] as! Date).description, "2024-10-17 14:10:17 +0000") // written to db: 2024-10-17T18:10:17
        XCTAssertEqual((resp![0]["date_ntz_ea"] as! Date).description, "2024-10-17 12:10:17 +0000") // written to db: 2024-10-17T16:10:17
        XCTAssertEqual((resp![0]["date_ntz_ny"] as! Date).description, "2024-10-17 22:10:17 +0000") // written to db: 2024-10-18T02:10:17
    }

    func checkReads_No_TZ_OfDubaiFromDubai_Fields_TZ() throws {
        let date_zz = newDateTime("2024-10-17 16:10:17", 0)
        let date_ea = newDateTime("2024-10-17 16:10:17", 14400) 
        let date_sp = newDateTime("2024-10-17 16:10:17", 7200)
        let date_ny = newDateTime("2024-10-17 16:10:17", -21600)
    
        let resp = try TimezoneTests.inst01?.executeQueryString(MDBQueryEncoderSQL(
                                                    MDBQuery(schemaTimezones + ".dubai").select().where().addCondition("id", .equal, uuidNoMDBTZ)
                                                    ).rawQuery())
        XCTAssertEqual(resp!.count, 1)
        if MIODBPostgreSQL.timeZoneBehaviourV2 {
            XCTAssertNotEqual(resp![0]["date_tz_zz"] as! Date, date_zz)
            XCTAssertNotEqual(resp![0]["date_tz_sp"] as! Date, date_sp)
            XCTAssertNotEqual(resp![0]["date_tz_ea"] as! Date, date_ea)
            XCTAssertNotEqual(resp![0]["date_tz_ny"] as! Date, date_ny)
            XCTAssertNotEqual((resp![0]["date_tz_zz"] as! Date).description, "2024-10-17 16:10:17 +0000") // written to db: 2024-10-17T20:10:17
            XCTAssertNotEqual((resp![0]["date_tz_sp"] as! Date).description, "2024-10-17 14:10:17 +0000") // written to db: 2024-10-17T18:10:17
            XCTAssertNotEqual((resp![0]["date_tz_ea"] as! Date).description, "2024-10-17 12:10:17 +0000") // written to db: 2024-10-17T16:10:17
            XCTAssertNotEqual((resp![0]["date_tz_ny"] as! Date).description, "2024-10-17 22:10:17 +0000") // written to db: 2024-10-18T02:10:17
        }
        else {
            XCTAssertEqual(resp![0]["date_tz_zz"] as! Date, date_zz)
            XCTAssertEqual(resp![0]["date_tz_sp"] as! Date, date_sp)
            XCTAssertEqual(resp![0]["date_tz_ea"] as! Date, date_ea)
            XCTAssertEqual(resp![0]["date_tz_ny"] as! Date, date_ny)
            XCTAssertEqual((resp![0]["date_tz_zz"] as! Date).description, "2024-10-17 16:10:17 +0000") // written to db: 2024-10-17T20:10:17
            XCTAssertEqual((resp![0]["date_tz_sp"] as! Date).description, "2024-10-17 14:10:17 +0000") // written to db: 2024-10-17T18:10:17
            XCTAssertEqual((resp![0]["date_tz_ea"] as! Date).description, "2024-10-17 12:10:17 +0000") // written to db: 2024-10-17T16:10:17
            XCTAssertEqual((resp![0]["date_tz_ny"] as! Date).description, "2024-10-17 22:10:17 +0000") // written to db: 2024-10-18T02:10:17
        }
    }

    func checkReads_No_TZ_OfSpainFromDubai_Fields_NTZ() throws {
        let date_zz = newDateTime("2024-10-17 16:10:17", 0)
        let date_ea = newDateTime("2024-10-17 16:10:17", 14400) 
        let date_sp = newDateTime("2024-10-17 16:10:17", 7200)
        let date_ny = newDateTime("2024-10-17 16:10:17", -21600)

        //let spainGmtOffset = TimeZone(identifier: "Europe/Madrid")!.secondsFromGMT()
        let dubaiSpainOffset = TimeZone(identifier: "Asia/Dubai")!.secondsFromGMT() - TimeZone(identifier: "Europe/Madrid")!.secondsFromGMT()

        let resp = try TimezoneTests.inst01?.executeQueryString(MDBQueryEncoderSQL(
                                                    MDBQuery(schemaTimezones + ".spain").select().where().addCondition("id", .equal, uuidNoMDBTZ)
                                                    ).rawQuery())
        if resp?.count ?? 0 > 0 {
            print("checkTimezonesFromDubai: checking spain values")
            XCTAssertEqual(resp!.count, 1)
            XCTAssertEqual(resp![0]["date_ntz_zz"] as! Date, date_zz.addingTimeInterval(TimeInterval(-dubaiSpainOffset)))
            XCTAssertEqual(resp![0]["date_ntz_sp"] as! Date, date_sp.addingTimeInterval(TimeInterval(-dubaiSpainOffset)))
            XCTAssertEqual(resp![0]["date_ntz_ea"] as! Date, date_ea.addingTimeInterval(TimeInterval(-dubaiSpainOffset)))
            XCTAssertEqual(resp![0]["date_ntz_ny"] as! Date, date_ny.addingTimeInterval(TimeInterval(-dubaiSpainOffset)))
            // When using MIOCore instead of parseDateTimeWithTimeZone() in convert() we get the same values here for the tz and ntz fields
            XCTAssertEqual((resp![0]["date_ntz_zz"] as! Date).description, "2024-10-17 14:10:17 +0000") // written to db: 2024-10-17T18:10:17 
            XCTAssertEqual((resp![0]["date_ntz_sp"] as! Date).description, "2024-10-17 12:10:17 +0000") // written to db: 2024-10-17T16:10:17
            XCTAssertEqual((resp![0]["date_ntz_ea"] as! Date).description, "2024-10-17 10:10:17 +0000") // written to db: 2024-10-17T14:10:17
            XCTAssertEqual((resp![0]["date_ntz_ny"] as! Date).description, "2024-10-17 20:10:17 +0000") // written to db: 2024-10-18T00:10:17
        }
    }

    func checkReads_No_TZ_OfSpainFromDubai_Fields_TZ() throws {
        let date_zz = newDateTime("2024-10-17 16:10:17", 0)
        let date_ea = newDateTime("2024-10-17 16:10:17", 14400) 
        let date_sp = newDateTime("2024-10-17 16:10:17", 7200)
        let date_ny = newDateTime("2024-10-17 16:10:17", -21600)

        let spainGmtOffset = TimeZone(identifier: "Europe/Madrid")!.secondsFromGMT()
        let dubaiSpainOffset = TimeZone(identifier: "Asia/Dubai")!.secondsFromGMT() - TimeZone(identifier: "Europe/Madrid")!.secondsFromGMT()

        let resp = try TimezoneTests.inst01?.executeQueryString(MDBQueryEncoderSQL(
                                                    MDBQuery(schemaTimezones + ".spain").select().where().addCondition("id", .equal, uuidNoMDBTZ)
                                                    ).rawQuery())
        if resp?.count ?? 0 > 0 {
            print("checkTimezonesFromDubai: checking spain values")
            XCTAssertEqual(resp!.count, 1)
            // This is what we should get (but we do not, hence the NotEqual)
            XCTAssertNotEqual(resp![0]["date_tz_zz"]  as! Date, date_zz)
            XCTAssertNotEqual(resp![0]["date_tz_sp"]  as! Date, date_sp)
            XCTAssertNotEqual(resp![0]["date_tz_ea"]  as! Date, date_ea)
            XCTAssertNotEqual(resp![0]["date_tz_ny"]  as! Date, date_ny)
            // This is what we really get when writing without MDBTZ to fields with timezone and then read from another timezone
            if MIODBPostgreSQL.timeZoneBehaviourV2 {
                XCTAssertEqual(resp![0]["date_tz_zz"]  as! Date, date_zz.addingTimeInterval(TimeInterval(spainGmtOffset)))
                XCTAssertEqual(resp![0]["date_tz_sp"]  as! Date, date_sp.addingTimeInterval(TimeInterval(spainGmtOffset)))
                XCTAssertEqual(resp![0]["date_tz_ea"]  as! Date, date_ea.addingTimeInterval(TimeInterval(spainGmtOffset)))
                XCTAssertEqual(resp![0]["date_tz_ny"]  as! Date, date_ny.addingTimeInterval(TimeInterval(spainGmtOffset)))
                // When using MIOCore instead of parseDateTimeWithTimeZone() in convert() we get the same values here for the tz and ntz fields
                XCTAssertEqual((resp![0]["date_tz_zz"]  as! Date).description, "2024-10-17 18:10:17 +0000") // written to db: 2024-10-17T18:10:17 
                XCTAssertEqual((resp![0]["date_tz_sp"]  as! Date).description, "2024-10-17 16:10:17 +0000") // written to db: 2024-10-17T16:10:17
                XCTAssertEqual((resp![0]["date_tz_ea"]  as! Date).description, "2024-10-17 14:10:17 +0000") // written to db: 2024-10-17T14:10:17
                XCTAssertEqual((resp![0]["date_tz_ny"]  as! Date).description, "2024-10-18 00:10:17 +0000") // written to db: 2024-10-18T00:10:17
            }
            else {
                XCTAssertEqual(resp![0]["date_tz_zz"]  as! Date, date_zz.addingTimeInterval(TimeInterval(-dubaiSpainOffset)))
                XCTAssertEqual(resp![0]["date_tz_sp"]  as! Date, date_sp.addingTimeInterval(TimeInterval(-dubaiSpainOffset)))
                XCTAssertEqual(resp![0]["date_tz_ea"]  as! Date, date_ea.addingTimeInterval(TimeInterval(-dubaiSpainOffset)))
                XCTAssertEqual(resp![0]["date_tz_ny"]  as! Date, date_ny.addingTimeInterval(TimeInterval(-dubaiSpainOffset)))
                // When using MIOCore instead of parseDateTimeWithTimeZone() in convert() we get the same values here for the tz and ntz fields
                XCTAssertEqual((resp![0]["date_tz_zz"]  as! Date).description, "2024-10-17 14:10:17 +0000") // written to db: 2024-10-17T18:10:17 
                XCTAssertEqual((resp![0]["date_tz_sp"]  as! Date).description, "2024-10-17 12:10:17 +0000") // written to db: 2024-10-17T16:10:17
                XCTAssertEqual((resp![0]["date_tz_ea"]  as! Date).description, "2024-10-17 10:10:17 +0000") // written to db: 2024-10-17T14:10:17
                XCTAssertEqual((resp![0]["date_tz_ny"]  as! Date).description, "2024-10-17 20:10:17 +0000") // written to db: 2024-10-18T00:10:17
            }
        }
    }

    func test_No_MDBTZ_Dubai() throws{
        try XCTSkipIf(SkipTestSuite, "TimezoneTests Suite not enabled")
        try XCTSkipIf(TimeZone.current.identifier != "Asia/Dubai", "test for Dubai timezone")
        let existenTablas = try existeTimezoneTables()
        if !existenTablas {
            try setupTimezoneTestsTables()
        }
        try checkInserts_No_TZ_FromDubai()
        try checkReads_No_TZ_OfDubaiFromDubai_Fields_NTZ()
        try checkReads_No_TZ_OfDubaiFromDubai_Fields_TZ()
        try checkReads_No_TZ_OfSpainFromDubai_Fields_NTZ()
        try checkReads_No_TZ_OfSpainFromDubai_Fields_TZ()
    }

// MARK: - MDBTZ spain

    func checkInsertsTZFromSpain() throws {
        let date_zz = newDateTime("2024-10-17 16:10:17", 0)      // 18:10:17
        let date_ea = newDateTime("2024-10-17 16:10:17", 14400)  // 14:10:17
        let date_sp = newDateTime("2024-10-17 16:10:17", 7200)   // 16:10:17
        let date_ny = newDateTime("2024-10-17 16:10:17", -21600) // 00:10:17
        let query1 = try MDBQuery( schemaTimezones + ".spain") { 
                Insert( [
                    "id": uuidMDBTZ,
                    "date_ntz_zz": date_zz,
                    "date_ntz_ea": date_ea,
                    "date_ntz_ny": date_ny,
                    "date_ntz_sp": date_sp,
                    "date_tz_zz": try MDBTZ(date_zz),
                    "date_tz_ea": try MDBTZ(date_ea),
                    "date_tz_ny": try MDBTZ(date_ny),
                    "date_tz_sp": try MDBTZ(date_sp),
                ] )
            }
        XCTAssertEqual(query1.values["date_ntz_zz"]?.value, "'2024-10-17T18:10:17'")
        XCTAssertEqual(query1.values["date_ntz_ea"]?.value, "'2024-10-17T14:10:17'")
        XCTAssertEqual(query1.values["date_ntz_sp"]?.value, "'2024-10-17T16:10:17'")
        XCTAssertEqual(query1.values["date_ntz_ny"]?.value, "'2024-10-18T00:10:17'")

        XCTAssertEqual(query1.values["date_tz_zz"]?.value,  "'2024-10-17T16:10:17Z'")
        XCTAssertEqual(query1.values["date_tz_ea"]?.value,  "'2024-10-17T12:10:17Z'")
        XCTAssertEqual(query1.values["date_tz_sp"]?.value,  "'2024-10-17T14:10:17Z'")
        XCTAssertEqual(query1.values["date_tz_ny"]?.value,  "'2024-10-17T22:10:17Z'")

        let data = try TimezoneTests.inst01?.executeQueryString(MDBQueryEncoderSQL(
                                                    MDBQuery(schemaTimezones + ".spain").select().where().addCondition("id", .equal, uuidMDBTZ)
                                                    ).rawQuery())
        if data!.count == 0 {  // no data yet. Insert
            // dubai: 2024-10-17 20:10:17 | 2024-10-17 16:10:17 | 2024-10-17 18:10:17+00
            try TimezoneTests.inst01?.executeQueryString(MDBQueryEncoderSQL(query1).rawQuery())
        }  
    }

    func checkReadsTZOfSpainFromSpain_Fields_NTZ() throws {
        let date_zz = newDateTime("2024-10-17 16:10:17", 0)
        let date_ea = newDateTime("2024-10-17 16:10:17", 14400) 
        let date_sp = newDateTime("2024-10-17 16:10:17", 7200)
        let date_ny = newDateTime("2024-10-17 16:10:17", -21600)

        let resp = try TimezoneTests.inst01?.executeQueryString(MDBQueryEncoderSQL(
                                                    MDBQuery(schemaTimezones + ".spain").select().where().addCondition("id", .equal, uuidMDBTZ)
                                                    ).rawQuery())
        XCTAssertEqual(resp!.count, 1)
        XCTAssertEqual(resp![0]["date_ntz_zz"] as! Date, date_zz)
        XCTAssertEqual(resp![0]["date_ntz_sp"] as! Date, date_sp)
        XCTAssertEqual(resp![0]["date_ntz_ea"] as! Date, date_ea)
        XCTAssertEqual(resp![0]["date_ntz_ny"] as! Date, date_ny)
        XCTAssertEqual((resp![0]["date_ntz_zz"] as! Date).description, "2024-10-17 16:10:17 +0000")
        XCTAssertEqual((resp![0]["date_ntz_sp"] as! Date).description, "2024-10-17 14:10:17 +0000")
        XCTAssertEqual((resp![0]["date_ntz_ea"] as! Date).description, "2024-10-17 12:10:17 +0000")
        XCTAssertEqual((resp![0]["date_ntz_ny"] as! Date).description, "2024-10-17 22:10:17 +0000")
    }

    func checkReadsTZOfSpainFromSpain_Fields_TZ() throws {
        let date_zz = newDateTime("2024-10-17 16:10:17", 0)
        let date_ea = newDateTime("2024-10-17 16:10:17", 14400) 
        let date_sp = newDateTime("2024-10-17 16:10:17", 7200)
        let date_ny = newDateTime("2024-10-17 16:10:17", -21600)

        let resp = try TimezoneTests.inst01?.executeQueryString(MDBQueryEncoderSQL(
                                                    MDBQuery(schemaTimezones + ".spain").select().where().addCondition("id", .equal, uuidMDBTZ)
                                                    ).rawQuery())
        XCTAssertEqual(resp!.count, 1)
        if MIODBPostgreSQL.timeZoneBehaviourV2 {
            XCTAssertEqual(resp![0]["date_tz_zz"] as! Date, date_zz)
            XCTAssertEqual(resp![0]["date_tz_sp"] as! Date, date_sp)
            XCTAssertEqual(resp![0]["date_tz_ea"] as! Date, date_ea)
            XCTAssertEqual(resp![0]["date_tz_ny"] as! Date, date_ny)
            XCTAssertEqual((resp![0]["date_tz_zz"] as! Date).description, "2024-10-17 16:10:17 +0000")
            XCTAssertEqual((resp![0]["date_tz_sp"] as! Date).description, "2024-10-17 14:10:17 +0000")
            XCTAssertEqual((resp![0]["date_tz_ea"] as! Date).description, "2024-10-17 12:10:17 +0000")
            XCTAssertEqual((resp![0]["date_tz_ny"] as! Date).description, "2024-10-17 22:10:17 +0000")
        }
        else {
            XCTAssertNotEqual(resp![0]["date_tz_zz"] as! Date, date_zz)
            XCTAssertNotEqual(resp![0]["date_tz_sp"] as! Date, date_sp)
            XCTAssertNotEqual(resp![0]["date_tz_ea"] as! Date, date_ea)
            XCTAssertNotEqual(resp![0]["date_tz_ny"] as! Date, date_ny)
            XCTAssertEqual((resp![0]["date_tz_zz"] as! Date).description, "2024-10-17 14:10:17 +0000")
            XCTAssertEqual((resp![0]["date_tz_sp"] as! Date).description, "2024-10-17 12:10:17 +0000")
            XCTAssertEqual((resp![0]["date_tz_ea"] as! Date).description, "2024-10-17 10:10:17 +0000")
            XCTAssertEqual((resp![0]["date_tz_ny"] as! Date).description, "2024-10-17 20:10:17 +0000")
        }
    }

    func checkReadsTZOfDubaiFromSpain_Fields_NTZ() throws {
        let date_zz = newDateTime("2024-10-17 16:10:17", 0)
        let date_ea = newDateTime("2024-10-17 16:10:17", 14400) 
        let date_sp = newDateTime("2024-10-17 16:10:17", 7200)
        let date_ny = newDateTime("2024-10-17 16:10:17", -21600)

        let dubaiSpainOffset = TimeZone(identifier: "Asia/Dubai")!.secondsFromGMT() - TimeZone(identifier: "Europe/Madrid")!.secondsFromGMT()

        let resp = try TimezoneTests.inst01?.executeQueryString(MDBQueryEncoderSQL(
                                                    MDBQuery(schemaTimezones + ".dubai").select().where().addCondition("id", .equal, uuidMDBTZ)
                                                    ).rawQuery())  
        if resp?.count ?? 0 > 0 {
            print("checkTimezonesFromSpain: checking dubai values")
            XCTAssertEqual(resp!.count, 1)
            XCTAssertEqual(resp![0]["date_ntz_zz"] as! Date, date_zz.addingTimeInterval(TimeInterval(dubaiSpainOffset)))
            XCTAssertEqual(resp![0]["date_ntz_sp"] as! Date, date_sp.addingTimeInterval(TimeInterval(dubaiSpainOffset)))
            XCTAssertEqual(resp![0]["date_ntz_ea"] as! Date, date_ea.addingTimeInterval(TimeInterval(dubaiSpainOffset)))
            XCTAssertEqual(resp![0]["date_ntz_ny"] as! Date, date_ny.addingTimeInterval(TimeInterval(dubaiSpainOffset)))
            XCTAssertEqual((resp![0]["date_ntz_zz"] as! Date).description, "2024-10-17 18:10:17 +0000") // written to db: 2024-10-17T20:10:17
            XCTAssertEqual((resp![0]["date_ntz_sp"] as! Date).description, "2024-10-17 16:10:17 +0000") // written to db: 2024-10-17T18:10:17
            XCTAssertEqual((resp![0]["date_ntz_ea"] as! Date).description, "2024-10-17 14:10:17 +0000") // written to db: 2024-10-17T16:10:17
            XCTAssertEqual((resp![0]["date_ntz_ny"] as! Date).description, "2024-10-18 00:10:17 +0000") // written to db: 2024-10-18T02:10:17
        }
    }

    func checkReadsTZOfDubaiFromSpain_Fields_TZ() throws {
        let date_zz = newDateTime("2024-10-17 16:10:17", 0)
        let date_ea = newDateTime("2024-10-17 16:10:17", 14400) 
        let date_sp = newDateTime("2024-10-17 16:10:17", 7200)
        let date_ny = newDateTime("2024-10-17 16:10:17", -21600)

        //let dubaiSpainOffset = TimeZone(identifier: "Asia/Dubai")!.secondsFromGMT() - TimeZone(identifier: "Europe/Madrid")!.secondsFromGMT()

        let resp = try TimezoneTests.inst01?.executeQueryString(MDBQueryEncoderSQL(
                                                    MDBQuery(schemaTimezones + ".dubai").select().where().addCondition("id", .equal, uuidMDBTZ)
                                                    ).rawQuery())  
        if resp?.count ?? 0 > 0 {
            print("checkTimezonesFromSpain: checking dubai values")
            XCTAssertEqual(resp!.count, 1)
            if MIODBPostgreSQL.timeZoneBehaviourV2 {
                // Now we get the correct values for the timezoned fields
                XCTAssertEqual(resp![0]["date_tz_zz"]  as! Date, date_zz)
                XCTAssertEqual(resp![0]["date_tz_sp"]  as! Date, date_sp)
                XCTAssertEqual(resp![0]["date_tz_ea"]  as! Date, date_ea)
                XCTAssertEqual(resp![0]["date_tz_ny"]  as! Date, date_ny)
                XCTAssertEqual((resp![0]["date_tz_zz"]  as! Date).description, "2024-10-17 16:10:17 +0000") // written to db: 2024-10-17T20:10:17
                XCTAssertEqual((resp![0]["date_tz_sp"]  as! Date).description, "2024-10-17 14:10:17 +0000") // written to db: 2024-10-17T18:10:17
                XCTAssertEqual((resp![0]["date_tz_ea"]  as! Date).description, "2024-10-17 12:10:17 +0000") // written to db: 2024-10-17T16:10:17
                XCTAssertEqual((resp![0]["date_tz_ny"]  as! Date).description, "2024-10-17 22:10:17 +0000") // written to db: 2024-10-18T02:10:17
            }
            else {
                XCTAssertNotEqual(resp![0]["date_tz_zz"]  as! Date, date_zz)
                XCTAssertNotEqual(resp![0]["date_tz_sp"]  as! Date, date_sp)
                XCTAssertNotEqual(resp![0]["date_tz_ea"]  as! Date, date_ea)
                XCTAssertNotEqual(resp![0]["date_tz_ny"]  as! Date, date_ny)
                XCTAssertEqual((resp![0]["date_tz_zz"]  as! Date).description, "2024-10-17 14:10:17 +0000") // written to db: 2024-10-17T20:10:17
                XCTAssertEqual((resp![0]["date_tz_sp"]  as! Date).description, "2024-10-17 12:10:17 +0000") // written to db: 2024-10-17T18:10:17
                XCTAssertEqual((resp![0]["date_tz_ea"]  as! Date).description, "2024-10-17 10:10:17 +0000") // written to db: 2024-10-17T16:10:17
                XCTAssertEqual((resp![0]["date_tz_ny"]  as! Date).description, "2024-10-17 20:10:17 +0000") // written to db: 2024-10-18T02:10:17
            }
        }
    }

    func test_With_MDBTZ_Madrid() throws{
        try XCTSkipIf(SkipTestSuite, "TimezoneTests Suite not enabled")
        try XCTSkipIf(TimeZone.current.identifier != "Europe/Madrid", "test for Madrid timezone")
        let existenTablas = try existeTimezoneTables()
        if !existenTablas {
            try setupTimezoneTestsTables()
        }
        try checkInsertsTZFromSpain()
        try checkReadsTZOfSpainFromSpain_Fields_NTZ()
        try checkReadsTZOfSpainFromSpain_Fields_TZ()
        try checkReadsTZOfDubaiFromSpain_Fields_NTZ()
        try checkReadsTZOfDubaiFromSpain_Fields_TZ()
    }


// MARK: - MDBTZ dubai
    func checkInsertsTZFromDubai() throws {
        let date_zz = newDateTime("2024-10-17 16:10:17", 0)
        let date_ea = newDateTime("2024-10-17 16:10:17", 14400) 
        let date_sp = newDateTime("2024-10-17 16:10:17", 7200)
        let date_ny = newDateTime("2024-10-17 16:10:17", -21600)
        let query1 = try MDBQuery( schemaTimezones + ".dubai") {
                Insert( [
                    "id": uuidMDBTZ,
                    "date_ntz_zz": date_zz,
                    "date_ntz_ea": date_ea,
                    "date_ntz_ny": date_ny,
                    "date_ntz_sp": date_sp,
                    "date_tz_zz": try MDBTZ(date_zz),
                    "date_tz_ea": try MDBTZ(date_ea),
                    "date_tz_ny": try MDBTZ(date_ny),
                    "date_tz_sp": try MDBTZ(date_sp),
                ] )
            }
        XCTAssertEqual(query1.values["date_ntz_zz"]?.value, "'2024-10-17T20:10:17'")
        XCTAssertEqual(query1.values["date_ntz_ea"]?.value, "'2024-10-17T16:10:17'")
        XCTAssertEqual(query1.values["date_ntz_sp"]?.value, "'2024-10-17T18:10:17'")
        XCTAssertEqual(query1.values["date_ntz_ny"]?.value, "'2024-10-18T02:10:17'")
        XCTAssertEqual(query1.values["date_tz_zz"]?.value,  "'2024-10-17T16:10:17Z'")
        XCTAssertEqual(query1.values["date_tz_ea"]?.value,  "'2024-10-17T12:10:17Z'")
        XCTAssertEqual(query1.values["date_tz_sp"]?.value,  "'2024-10-17T14:10:17Z'")
        XCTAssertEqual(query1.values["date_tz_ny"]?.value,  "'2024-10-17T22:10:17Z'")

        let data = try TimezoneTests.inst01?.executeQueryString(MDBQueryEncoderSQL(
                                                    MDBQuery(schemaTimezones + ".dubai").select().where().addCondition("id", .equal, uuidMDBTZ)
                                                    ).rawQuery())
        if data!.count == 0 {  // no data yet. Insert
            try TimezoneTests.inst01?.executeQueryString(MDBQueryEncoderSQL(query1).rawQuery())
        }       
    }

    func checkReadsTZOfDubaiFromDubai_Fields_NTZ() throws {
        let date_zz = newDateTime("2024-10-17 16:10:17", 0)
        let date_ea = newDateTime("2024-10-17 16:10:17", 14400) 
        let date_sp = newDateTime("2024-10-17 16:10:17", 7200)
        let date_ny = newDateTime("2024-10-17 16:10:17", -21600)
    
        let resp = try TimezoneTests.inst01?.executeQueryString(MDBQueryEncoderSQL(
                                                    MDBQuery(schemaTimezones + ".dubai").select().where().addCondition("id", .equal, uuidMDBTZ)
                                                    ).rawQuery())
        XCTAssertEqual(resp!.count, 1)
        XCTAssertEqual(resp![0]["date_ntz_zz"] as! Date, date_zz)
        XCTAssertEqual(resp![0]["date_ntz_sp"] as! Date, date_sp)
        XCTAssertEqual(resp![0]["date_ntz_ea"] as! Date, date_ea)
        XCTAssertEqual(resp![0]["date_ntz_ny"] as! Date, date_ny)
        XCTAssertEqual((resp![0]["date_ntz_zz"] as! Date).description, "2024-10-17 16:10:17 +0000")
        XCTAssertEqual((resp![0]["date_ntz_sp"] as! Date).description, "2024-10-17 14:10:17 +0000")
        XCTAssertEqual((resp![0]["date_ntz_ea"] as! Date).description, "2024-10-17 12:10:17 +0000")
        XCTAssertEqual((resp![0]["date_ntz_ny"] as! Date).description, "2024-10-17 22:10:17 +0000")
    }

    func checkReadsTZOfDubaiFromDubai_Fields_TZ() throws {
        let date_zz = newDateTime("2024-10-17 16:10:17", 0)
        let date_ea = newDateTime("2024-10-17 16:10:17", 14400) 
        let date_sp = newDateTime("2024-10-17 16:10:17", 7200)
        let date_ny = newDateTime("2024-10-17 16:10:17", -21600)
    
        let resp = try TimezoneTests.inst01?.executeQueryString(MDBQueryEncoderSQL(
                                                    MDBQuery(schemaTimezones + ".dubai").select().where().addCondition("id", .equal, uuidMDBTZ)
                                                    ).rawQuery())
        XCTAssertEqual(resp!.count, 1)
        if MIODBPostgreSQL.timeZoneBehaviourV2 {
            XCTAssertEqual(resp![0]["date_tz_zz"] as! Date, date_zz)
            XCTAssertEqual(resp![0]["date_tz_sp"] as! Date, date_sp)
            XCTAssertEqual(resp![0]["date_tz_ea"] as! Date, date_ea)
            XCTAssertEqual(resp![0]["date_tz_ny"] as! Date, date_ny)
            XCTAssertEqual((resp![0]["date_tz_zz"] as! Date).description, "2024-10-17 16:10:17 +0000")
            XCTAssertEqual((resp![0]["date_tz_sp"] as! Date).description, "2024-10-17 14:10:17 +0000")
            XCTAssertEqual((resp![0]["date_tz_ea"] as! Date).description, "2024-10-17 12:10:17 +0000")
            XCTAssertEqual((resp![0]["date_tz_ny"] as! Date).description, "2024-10-17 22:10:17 +0000")
        }
        else {
            XCTAssertNotEqual(resp![0]["date_tz_zz"] as! Date, date_zz)
            XCTAssertNotEqual(resp![0]["date_tz_sp"] as! Date, date_sp)
            XCTAssertNotEqual(resp![0]["date_tz_ea"] as! Date, date_ea)
            XCTAssertNotEqual(resp![0]["date_tz_ny"] as! Date, date_ny)
            XCTAssertEqual((resp![0]["date_tz_zz"] as! Date).description, "2024-10-17 12:10:17 +0000")
            XCTAssertEqual((resp![0]["date_tz_sp"] as! Date).description, "2024-10-17 10:10:17 +0000")
            XCTAssertEqual((resp![0]["date_tz_ea"] as! Date).description, "2024-10-17 08:10:17 +0000")
            XCTAssertEqual((resp![0]["date_tz_ny"] as! Date).description, "2024-10-17 18:10:17 +0000")
        }
    }

    func checkReadsTZOfSpainFromDubai_Fields_NTZ() throws {
        let date_zz = newDateTime("2024-10-17 16:10:17", 0)
        let date_ea = newDateTime("2024-10-17 16:10:17", 14400) 
        let date_sp = newDateTime("2024-10-17 16:10:17", 7200)
        let date_ny = newDateTime("2024-10-17 16:10:17", -21600)

        let dubaiSpainOffset = TimeZone(identifier: "Asia/Dubai")!.secondsFromGMT() - TimeZone(identifier: "Europe/Madrid")!.secondsFromGMT()

        let resp = try TimezoneTests.inst01?.executeQueryString(MDBQueryEncoderSQL(
                                                    MDBQuery(schemaTimezones + ".spain").select().where().addCondition("id", .equal, uuidMDBTZ)
                                                    ).rawQuery())
        if resp?.count ?? 0 > 0 {
            print("checkTimezonesFromDubai: cheking spain values")
            XCTAssertEqual(resp!.count, 1)
            XCTAssertEqual(resp![0]["date_ntz_zz"] as! Date, date_zz.addingTimeInterval(TimeInterval(-dubaiSpainOffset)))
            XCTAssertEqual(resp![0]["date_ntz_sp"] as! Date, date_sp.addingTimeInterval(TimeInterval(-dubaiSpainOffset)))
            XCTAssertEqual(resp![0]["date_ntz_ea"] as! Date, date_ea.addingTimeInterval(TimeInterval(-dubaiSpainOffset)))
            XCTAssertEqual(resp![0]["date_ntz_ny"] as! Date, date_ny.addingTimeInterval(TimeInterval(-dubaiSpainOffset)))
            XCTAssertEqual((resp![0]["date_ntz_zz"] as! Date).description, "2024-10-17 14:10:17 +0000")
            XCTAssertEqual((resp![0]["date_ntz_sp"] as! Date).description, "2024-10-17 12:10:17 +0000")
            XCTAssertEqual((resp![0]["date_ntz_ea"] as! Date).description, "2024-10-17 10:10:17 +0000")
            XCTAssertEqual((resp![0]["date_ntz_ny"] as! Date).description, "2024-10-17 20:10:17 +0000")
        }
    }

    func checkReadsTZOfSpainFromDubai_Fields_TZ() throws {
        let date_zz = newDateTime("2024-10-17 16:10:17", 0)
        let date_ea = newDateTime("2024-10-17 16:10:17", 14400) 
        let date_sp = newDateTime("2024-10-17 16:10:17", 7200)
        let date_ny = newDateTime("2024-10-17 16:10:17", -21600)

        //let dubaiSpainOffset = TimeZone(identifier: "Asia/Dubai")!.secondsFromGMT() - TimeZone(identifier: "Europe/Madrid")!.secondsFromGMT()

        let resp = try TimezoneTests.inst01?.executeQueryString(MDBQueryEncoderSQL(
                                                    MDBQuery(schemaTimezones + ".spain").select().where().addCondition("id", .equal, uuidMDBTZ)
                                                    ).rawQuery())
        if resp?.count ?? 0 > 0 {
            print("checkTimezonesFromDubai: cheking spain values")
            XCTAssertEqual(resp!.count, 1)
            if MIODBPostgreSQL.timeZoneBehaviourV2 {
                // Now we get the correct values for the timezoned fields
                XCTAssertEqual(resp![0]["date_tz_zz"]  as! Date, date_zz)
                XCTAssertEqual(resp![0]["date_tz_sp"]  as! Date, date_sp)
                XCTAssertEqual(resp![0]["date_tz_ea"]  as! Date, date_ea)
                XCTAssertEqual(resp![0]["date_tz_ny"]  as! Date, date_ny)
                XCTAssertEqual((resp![0]["date_tz_zz"]  as! Date).description, "2024-10-17 16:10:17 +0000")
                XCTAssertEqual((resp![0]["date_tz_sp"]  as! Date).description, "2024-10-17 14:10:17 +0000")
                XCTAssertEqual((resp![0]["date_tz_ea"]  as! Date).description, "2024-10-17 12:10:17 +0000")
                XCTAssertEqual((resp![0]["date_tz_ny"]  as! Date).description, "2024-10-17 22:10:17 +0000")
            }
            else {
                XCTAssertNotEqual(resp![0]["date_tz_zz"]  as! Date, date_zz)
                XCTAssertNotEqual(resp![0]["date_tz_sp"]  as! Date, date_sp)
                XCTAssertNotEqual(resp![0]["date_tz_ea"]  as! Date, date_ea)
                XCTAssertNotEqual(resp![0]["date_tz_ny"]  as! Date, date_ny)
                XCTAssertEqual((resp![0]["date_tz_zz"]  as! Date).description, "2024-10-17 12:10:17 +0000")
                XCTAssertEqual((resp![0]["date_tz_sp"]  as! Date).description, "2024-10-17 10:10:17 +0000")
                XCTAssertEqual((resp![0]["date_tz_ea"]  as! Date).description, "2024-10-17 08:10:17 +0000")
                XCTAssertEqual((resp![0]["date_tz_ny"]  as! Date).description, "2024-10-17 18:10:17 +0000")
            }
        }
    }

    func test_With_MDBTZ_Dubai() throws{
        try XCTSkipIf(SkipTestSuite, "TimezoneTests Suite not enabled")
        try XCTSkipIf(TimeZone.current.identifier != "Asia/Dubai", "test for Dubai timezone")
        let existenTablas = try existeTimezoneTables()
        if !existenTablas {
            try setupTimezoneTestsTables()
        }
        try checkInsertsTZFromDubai()
        try checkReadsTZOfDubaiFromDubai_Fields_NTZ()
        try checkReadsTZOfDubaiFromDubai_Fields_TZ()
        try checkReadsTZOfSpainFromDubai_Fields_NTZ()
        try checkReadsTZOfSpainFromDubai_Fields_TZ()
    }

// MARK: - date parser 

    func test_ParsingFromPostgre_Not_timeZoneBehaviourV2() throws {
        let date_ntz_sp = localMIOCoreDate(fromString: "2024-10-17 16:10:17")    // PostgreSQL Type: 1114 Value: 2024-10-17 16:10:17
        let date_tz_sp  = localMIOCoreDate(fromString: "2024-10-17 16:10:17+00") // PostgreSQL Type: 1184 Value: 2024-10-17 16:10:17+00
        if TimeZone.current.identifier == "Europe/Madrid" {
            XCTAssertEqual(date_ntz_sp!.description, "2024-10-17 14:10:17 +0000")
            XCTAssertEqual(date_tz_sp!.description,  "2024-10-17 14:10:17 +0000")
        }
        if TimeZone.current.identifier == "Asia/Dubai" {
            XCTAssertEqual(date_ntz_sp!.description, "2024-10-17 12:10:17 +0000")
            XCTAssertEqual(date_tz_sp!.description,  "2024-10-17 12:10:17 +0000")
        }
    }

     

    func test_ParsingWithTimezone() throws {
        let d1 = MIODBPostgreSQL.parseDateTimeWithTimeZone("2023-10-21T14:30:00Z")
        let d2 = MIODBPostgreSQL.parseDateTimeWithTimeZone("2023-10-21T14:30:00+02:00")
        let d3 = MIODBPostgreSQL.parseDateTimeWithTimeZone("2023-10-21T14:30:00-02:00")
        let d4 = MIODBPostgreSQL.parseDateTimeWithTimeZone("2023-10-21 14:30:00+02:00")
        let d5 = MIODBPostgreSQL.parseDateTimeWithTimeZone("2023-10-21T14:30:00+00")
        let d6 = MIODBPostgreSQL.parseDateTimeWithTimeZone("2023-10-21 14:30:00+02")
        let d7 = MIODBPostgreSQL.parseDateTimeWithTimeZone("2023-10-21 14:30:00Z")
        let d8 = MIODBPostgreSQL.parseDateTimeWithTimeZone("2024-10-18 11:47:33.476945+00")

        XCTAssertEqual(d1!.description, "2023-10-21 14:30:00 +0000")
        XCTAssertEqual(d2!.description, "2023-10-21 12:30:00 +0000")
        XCTAssertEqual(d3!.description, "2023-10-21 16:30:00 +0000")
        XCTAssertEqual(d4!.description, "2023-10-21 12:30:00 +0000")
        XCTAssertEqual(d5!.description, "2023-10-21 14:30:00 +0000")
        XCTAssertEqual(d6!.description, "2023-10-21 12:30:00 +0000")
        XCTAssertEqual(d7!.description, "2023-10-21 14:30:00 +0000")
        XCTAssertEqual(d8!.description, "2024-10-18 11:47:33 +0000")
    }

// MARK: - Full cycle 
    // func test_TimeZoneAssessment() throws {
    //     try TimezoneTests.inst01?.executeQueryString("DROP SCHEMA IF EXISTS sch1 CASCADE")
    //     try TimezoneTests.inst01?.executeQueryString("CREATE SCHEMA sch1")
    //     let tabla = "CREATE TABLE sch1.tabla (id INT PRIMARY KEY, ntz TIMESTAMP, tz TIMESTAMP WITH TIME ZONE)"
    //     try TimezoneTests.inst01?.executeQueryString(tabla)

    //     //let d = Date()
        
    //     let df1 = DateFormatter()
    //     df1.timeZone = TimeZone(identifier: "Asia/Dubai")
    //     //df1.timeZone = TimeZone(secondsFromGMT: 0)
    //     df1.dateFormat = "yyyy-MM-dd HH:mm:ss"
    //     let d1 = df1.date(from: "2024-10-17 16:10:17")!
        
    //     let df2 = DateFormatter()
    //     df2.dateFormat = "yyyy-MM-dd HH:mm:ss"
    //     //df2.timeZone = TimeZone(secondsFromGMT: 0)
    //     df2.timeZone = TimeZone(identifier: "Europe/Madrid")
    //     let d2 = df2.date(from: "2024-10-17 16:10:17")!
        
    //     let d1_tz = mcd_date_time_formatter_s().string(from: d1)
    //     let d2_tz = mcd_date_time_formatter_s().string(from: d2)
        
    //     let df3 = DateFormatter()
    //     df3.dateFormat = "yyyy-MM-dd HH:mm:ss"
    //     df3.timeZone = TimeZone(secondsFromGMT: 0)
        
    //     let c = Calendar.current.dateComponents([.day, .hour, .minute, .timeZone], from: d1)
        
    //     let d1_ntz = "2024-10-17 16:10:17"
    //     let d2_ntz = "2024-10-17 16:10:17"
        
    //     let date1 = d1//mcd_date_time_formatter_s().date(from: "2024-10-17 16:10:17" ) // newDateTime("2024-10-17 16:10:17", 7200)
    //     let date2 = d2 //mcd_date_time_formatter_s().date(from: "2024-10-17 18:10:17" ) // newDateTime("2024-10-17 18:10:17", 7200)
    //     let query1 = try MDBQuery("sch1.tabla").insert( ["id":1, "ntz": d1_ntz, "tz": date1] )
    //     let query2 = try MDBQuery("sch1.tabla").insert( ["id":2, "ntz": d2_ntz, "tz": date2] )

    //     XCTAssertEqual(query1.values["ntz"]?.value, "'2024-10-17T16:10:17'")
    //     XCTAssertEqual(query2.values["ntz"]?.value, "'2024-10-17T18:10:17'")
    //     try TimezoneTests.inst01?.executeQueryString(MDBQueryEncoderSQL(query1).rawQuery())
    //     try TimezoneTests.inst01?.executeQueryString(MDBQueryEncoderSQL(query2).rawQuery())

    //     let data = try TimezoneTests.inst01?.executeQueryString(MDBQueryEncoderSQL(MDBQuery("sch1.tabla").select().orderBy("id")).rawQuery())
    //     XCTAssertEqual(data!.count, 2)
    //     XCTAssertEqual((data![0]["ntz"] as! Date), date1)
    //     XCTAssertEqual((data![1]["ntz"] as! Date), date2)
    //     XCTAssertEqual(mcd_date_time_formatter_s().string(from: (data![0]["ntz"] as! Date)), "2024-10-17 16:10:17")
    //     XCTAssertEqual(mcd_date_time_formatter_s().string(from: (data![1]["ntz"] as! Date)), "2024-10-17 18:10:17")
    //     XCTAssertEqual((data![0]["ntz"] as! Date).description, "2024-10-17 14:10:17 +0000")
    //     XCTAssertEqual((data![1]["ntz"] as! Date).description, "2024-10-17 16:10:17 +0000")
    // }
}

// MARK: - mioCore global
// Extract of the relevant behaviour of MIOCore in the process of parsing datatimes (timestamps) received from postgreSQL 
fileprivate var _mcd_date_time_formatter_s:DateFormatter?
fileprivate func mcd_date_time_formatter_s() -> DateFormatter {
    if _mcd_date_time_formatter_s == nil {
        let df = DateFormatter()
        df.locale = Locale(identifier: "en_US_POSIX")
        df.dateFormat = "yyyy-MM-dd HH:mm:ss"
        _mcd_date_time_formatter_s = df
    }
    return _mcd_date_time_formatter_s!
}


fileprivate func localMIOCoreDate(fromString dateString: String ) -> Date?
{
    var date:Date?

        var df:DateFormatter
                
        // Most probably case
        df = mcd_date_time_formatter_s()
        if let ret = df.date(from: dateString ) { date = ret; return ret} // return point for timestamp without timezone data type
        
        let rm_ms    = String( dateString.split( separator: "." )[ 0 ] )
        var last_try = rm_ms.replacingOccurrences( of: "T", with: " " )
        
        if last_try.count > 19 {
            last_try = String( last_try[..<last_try.index(last_try.startIndex, offsetBy: 19)] )
            
        }

        df = mcd_date_time_formatter_s()
        if let ret = df.date(from: last_try ) { date = ret; return ret} // return point for timestamp with timezone data type
        
    return date

}

