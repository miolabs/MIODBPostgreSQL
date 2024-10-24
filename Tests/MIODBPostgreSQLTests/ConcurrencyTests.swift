
import XCTest
@testable import MIODB
@testable import MIODBPostgreSQL
import CLibPQ

// ----------------------------------
// Concurrency tests for postgreSQL
// ----------------------------------
//
// The goal is to check the behaviour with multiple threads working at the same time on the same or different schemas.
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
// Now you can run these tests. Check out the values of the following MIODBPostgreSQLTests class properties:
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
// - If you need to query directly, this is the schema pattern:
//      _00000000000000000000000000010000.tabla3

final class ConcurrencyTests: XCTestCase {
    //// Set to true to skip the test suite 
    private let SkipTestSuite = true  // We could use defines at project level instead of this
    //// MongoDB connection data
    private static let host: String  = "localhost"
    private static let user: String  = "user"
    private static let pass: String  = "pass"
    private static let db01: String  = "DBTest01"
    private static let port01: Int32 = 5432

    private static var conn01 : MDBPostgreConnection? = nil
    private static var inst01 : MIODBPostgreSQL? = nil

    private var schemas: [String] = []
    private var threadIds: [Int64] = []
    private let mutex = DispatchSemaphore(value: 1)
    private let uuidBut4 = "00000000-0000-0000-0000-00000001"
    private let schemaBut4 = "_0000000000000000000000000001"

    override class func setUp() {
        conn01 = MDBPostgreConnection(host: ConcurrencyTests.host, port: ConcurrencyTests.port01, user: ConcurrencyTests.user, password: ConcurrencyTests.pass)
        do {
            inst01 = try conn01?.create(db01) as? MIODBPostgreSQL
            //try ConcurrencyTests.inst!.dropCollection(ConcurrencyTests.testCollection)

        } catch {
            XCTFail("Error setting up test suite: cant connect to postgreSQL database")
        }
        super.setUp()
    }

    override class func tearDown() {
        ConcurrencyTests.inst01?.disconnect()
        super.tearDown()
    }

// MARK: - utils schema    

    func dropAllSchemas(_ db: MIODBPostgreSQL) throws {
        schemas.removeAll()
        threadIds.removeAll()

        let rows = try db.executeQueryString("SELECT schema_name FROM information_schema.schemata")
        for row in rows! {
            let schema = row["schema_name"] as! String
            if schema.starts(with: schemaBut4) {
                try db.executeQueryString("DROP SCHEMA IF EXISTS \(schema) CASCADE")
            }
        }
    }

    func getCurrentThreadID() -> Int64 {
        let threadID = pthread_self() 
        let threadIDValue = UInt(bitPattern: threadID) 
        return Int64(threadIDValue)
        //print("Thread ID: \(threadIDValue)")
    }

    func setupTables(_ db: MIODBPostgreSQL, _ schema: String) throws {
       let tabla1 = """
                CREATE TABLE esquema.tabla1 (
                    id UUID PRIMARY KEY,
                    nombre TEXT NOT NULL,
                    valor NUMERIC(10, 0),
                    schema_id INTEGER,
                    contador SERIAL,
                    thread bigint,
                    fecha_creacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP);
                """
        let tabla2 = """
                CREATE TABLE esquema.tabla2 (
                    id UUID PRIMARY KEY,
                    descripcion TEXT NOT NULL,
                    cantidad NUMERIC(8, 3),
                    valor2 NUMERIC(10, 0),
                    tabla1_id UUID REFERENCES esquema.tabla1(id));
                """
        let tabla3 = """
                CREATE TABLE esquema.tabla3 (
                    id UUID PRIMARY KEY,
                    date_ntz1 TIMESTAMP,
                    date_ntz2 TIMESTAMP WITHOUT TIME ZONE,
                    date_tz TIMESTAMP WITH TIME ZONE,
                    date_creation_ntz TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    date_creation_tz TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
                    );
                """
        try db.executeQueryString(tabla1.replacingOccurrences(of: "esquema", with: schema))
        try db.executeQueryString(tabla2.replacingOccurrences(of: "esquema", with: schema))
        try db.executeQueryString(tabla3.replacingOccurrences(of: "esquema", with: schema))

        // CREATE SEQUENCE esquema.mi_secuencia START 1;

        // CREATE TABLE esquema.tabla1 (
        //     id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        //     nombre TEXT NOT NULL,
        //     valor NUMERIC(10, 2),
        //     contador INTEGER DEFAULT nextval('esquema.mi_secuencia'),  -- Usar la secuencia manualmente
        //     fecha_creacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP);
    }

    func getSchemaString(_ i: Int) -> String {
        let scheme = "_" + uuidBut4 + String(format: "%04d", i)
        return scheme.replacingOccurrences(of: "-", with: "")
    }

    func setupDatabase(_ db: MIODBPostgreSQL, _ numberOfSchemes: Int) throws {
        for i in 0..<numberOfSchemes {
            let scheme = getSchemaString(i)
            try db.executeQueryString("CREATE SCHEMA \(scheme)")
            try setupTables(db, scheme)
            schemas.append(scheme)
        }
    }

    @discardableResult
    func createSchemas(numberOfSchemes: Int) throws -> Bool {
        guard let db = ConcurrencyTests.inst01 else {
            XCTFail("Error getting database instance")
            return false
        }
        try dropAllSchemas(db)
        let rowsBefore = try db.executeQueryString("SELECT schema_name FROM information_schema.schemata")
        try setupDatabase(db, numberOfSchemes)
        let rows = try db.executeQueryString("SELECT schema_name FROM information_schema.schemata")
        let createdOk: Bool = rows?.count ==  numberOfSchemes + rowsBefore!.count 
        XCTAssertTrue(createdOk)
        XCTAssertEqual(schemas.count, numberOfSchemes)
        return createdOk
    }

 // MARK: - inserts    

    func doInserts(_ i: Int, _ numInserts: Int) throws {
        do {
            let threadID = getCurrentThreadID()
            let db = try ConcurrencyTests.conn01!.create(ConcurrencyTests.db01) as! MIODBPostgreSQL
            let schema = getSchemaString(i)
            for j in 0..<numInserts {
                let id1 = UUID().uuidString
                let id2 = UUID().uuidString
                let valor = j * 100
                let query1 = try MDBQuery( schema + ".tabla1") {
                    Insert( [
                        "id": id1,
                        "nombre": "Nombre \(j)",
                        "valor": valor,
                        "schema_id": i,
                        "thread": threadID,
                    ] )
                }
                try db.executeQueryString(MDBQueryEncoderSQL(query1).rawQuery())

                let query2 = try MDBQuery( schema + ".tabla2") {
                    Insert( [
                        "id": id2,
                        "descripcion": "Descripcion \(j)",
                        "cantidad": Double(valor) + 0.25,
                        "valor2": valor,
                        "tabla1_id": id1,
                    ] )
                }
                try db.executeQueryString(MDBQueryEncoderSQL(query2).rawQuery())
            }
            mutex.wait()
            threadIds.append(threadID)
            mutex.signal()
            db.disconnect()
        }
        catch {
            //print("ERROR +++ Thread \(i)")
            XCTFail("Error in thread \(i): \(error.localizedDescription)")
        }
        //print("Thread \(i) finished")
    }

// MARK: - independent  
    func checkTabla1IndependentInserts(_ db: MIODBPostgreSQL, _ schemaNumber: Int, _ insertsPerThread: Int) throws {
        let schema = getSchemaString(schemaNumber)
        let query = MDBQuery( schema + ".tabla1").select().orderBy("fecha_creacion", .ASC)
        let rows = try db.executeQueryString(MDBQueryEncoderSQL(query).rawQuery())
        XCTAssertEqual(rows!.count, insertsPerThread)
        var lastValor : Int32 = -1;
        var thread : Int64 = 0
        for i in 0..<rows!.count {
            let row = rows![i]
            let contador = row["contador"] as! Int32
            XCTAssertTrue(contador > lastValor)
            XCTAssertEqual(row["nombre"] as! String, "Nombre \(i)")
            XCTAssertEqual(row["valor"] as! Decimal, Decimal(i * 100))
            XCTAssertEqual(row["schema_id"] as! Int32, Int32(schemaNumber))
            let threadRow = row["thread"] as! Int64
            XCTAssertTrue(threadIds.contains(threadRow)) // thread id is in the list of threads
            
            if i == 0 {
                thread = threadRow
            }
            else {
                XCTAssertEqual(threadRow, thread) // all rows from the same thread
            }
            lastValor = contador
        }
        if let index = threadIds.firstIndex(of: thread) {
            threadIds.remove(at: index)
        }
    }
    
    func checkIndependentInserts(_ numberOfThreads: Int, _ insertsPerThread: Int) throws{
        XCTAssertTrue(threadIds.count == numberOfThreads)
        for i in 0..<numberOfThreads { 
            //print("Checking thread \(i) ")
            let db = ConcurrencyTests.inst01!
            
            try checkTabla1IndependentInserts(db, i, insertsPerThread)

            // let query2 = try MDBQuery( schema + ".tabla2") {
            //     Select( [ "id", "descripcion", "cantidad", "valor2", "tabla1_id" ] )
            // }
            // let rows2 = try db.executeQueryString(MDBQueryEncoderSQL(query2).rawQuery())
            // XCTAssertEqual(rows2!.count, insertsPerThread)
        }
        XCTAssertTrue(threadIds.count == 0) // all threads were in the list
    }

    func independentInsert_Threads(numberOfThreads: Int, insertsPerThread: Int) throws{
        try createSchemas(numberOfSchemes: numberOfThreads)

        let semaphore = DispatchSemaphore(value: 0) 
        for i in 0..<numberOfThreads { 
            //print("Dispatching thread \(i) ")
            let thread = Thread {
                try? self.doInserts(i, insertsPerThread)
                semaphore.signal()
            }
            thread.start()
        }
        // Esperar a que todos los threads terminen
        for _ in 0..<numberOfThreads {
            semaphore.wait()
        }
        try checkIndependentInserts(numberOfThreads, insertsPerThread)
    }

    func independentInsert_DispatchQueue(numberOfThreads: Int, insertsPerThread: Int) throws {
        try createSchemas(numberOfSchemes: numberOfThreads)
        
        let dispatchGroup = DispatchGroup()
        for i in 0..<numberOfThreads {
            dispatchGroup.enter() 
            //print("Dispatching thread \(i) ")
            DispatchQueue.global().async {
                try? self.doInserts(i, insertsPerThread)
                dispatchGroup.leave() 
            }
        }
        dispatchGroup.wait()
        try checkIndependentInserts(numberOfThreads, insertsPerThread)
    }

    func testIndependentSchemas_025_Threads() throws{
        try XCTSkipIf(SkipTestSuite, "Life Test Suite not enabled")
        try independentInsert_Threads(numberOfThreads: 25, insertsPerThread: 100)
    }
    func testIndependentSchemas_100_Threads() throws{
        try XCTSkipIf(SkipTestSuite, "Life Test Suite not enabled")
        try independentInsert_Threads(numberOfThreads: 100, insertsPerThread: 100)
    }
    func testIndependentSchemas_200_Threads() throws{
        try XCTSkipIf(SkipTestSuite, "Life Test Suite not enabled")
        try independentInsert_Threads(numberOfThreads: 200, insertsPerThread: 80)
    }
    func testIndependentSchemas_500_Threads() throws{
        try XCTSkipIf(SkipTestSuite, "Life Test Suite not enabled")
        try independentInsert_Threads(numberOfThreads: 500, insertsPerThread: 50)
    }
    func testIndependentSchemas_025_DispatchQ() throws{
        try XCTSkipIf(SkipTestSuite, "Life Test Suite not enabled")
        try independentInsert_DispatchQueue(numberOfThreads: 25, insertsPerThread: 100)
    }
    func testIndependentSchemas_100_DispatchQ() throws{
        try XCTSkipIf(SkipTestSuite, "Life Test Suite not enabled")
        try independentInsert_DispatchQueue(numberOfThreads: 100, insertsPerThread: 100)
    }
    func testIndependentSchemas_200_DispatchQ() throws{
        try XCTSkipIf(SkipTestSuite, "Life Test Suite not enabled")
        try independentInsert_DispatchQueue(numberOfThreads: 200, insertsPerThread: 80)
    }
    func testIndependentSchemas_500_DispatchQ() throws{
        try XCTSkipIf(SkipTestSuite, "Life Test Suite not enabled")
        try independentInsert_DispatchQueue(numberOfThreads: 500, insertsPerThread: 50)
    }

// MARK: - same schm      
    func checkTabla1SameSchemaInserts(_ db: MIODBPostgreSQL, _ schemaNumber: Int, _ threadsPerGroup: Int, _ insertsPerThread: Int) throws {
        let schema = getSchemaString(schemaNumber)
        let query = MDBQuery( schema + ".tabla1").select().orderBy("fecha_creacion", .ASC)
        let rows = try db.executeQueryString(MDBQueryEncoderSQL(query).rawQuery())
        XCTAssertEqual(rows!.count, threadsPerGroup * insertsPerThread)
        //var lastValor : Int32 = -1;
        var threadInserts : [Int64: Int] = [:]
        for i in 0..<rows!.count {
            let row = rows![i]
            //let contador = row["contador"] as! Int32
            //XCTAssertTrue(contador > lastValor) This is not true any more. We have to order by date or serial, both may not work in multi-thread
            XCTAssertEqual(row["schema_id"] as! Int32, Int32(schemaNumber))
            let threadRow = row["thread"] as! Int64
            XCTAssertTrue(threadIds.contains(threadRow)) // thread id is in the list of threads
            if let count = threadInserts[threadRow] {
                threadInserts[threadRow] = count + 1
            }
            else {
                threadInserts[threadRow] = 1
            }
            //lastValor = contador
        }
        for (thread, count) in threadInserts {
            XCTAssertEqual(count, insertsPerThread)
            if let index = threadIds.firstIndex(of: thread) {
                threadIds.remove(at: index)
            }
        }
    }
    
    func checkSameSchemaInserts(_ numberOfGroups: Int, _ threadsPerGroup: Int, _ insertsPerThread: Int) throws{
        let numberOfThreads = numberOfGroups * threadsPerGroup

        XCTAssertTrue(threadIds.count == numberOfThreads)
        
        for i in 0..<numberOfGroups { 
            //print("Checking thread \(i) ")
            let db = ConcurrencyTests.inst01!
            
            try checkTabla1SameSchemaInserts(db, i, threadsPerGroup, insertsPerThread)
        }
         XCTAssertTrue(threadIds.count == 0) // all threads were in the list
    }

    func sameSchemaInsert_Threads(numberOfGroups: Int, threadsPerGroup: Int, insertsPerThread: Int) throws{
        try createSchemas(numberOfSchemes: numberOfGroups)

        let numberOfThreads = numberOfGroups * threadsPerGroup
        let semaphore = DispatchSemaphore(value: 0) 
        for i in 0..<numberOfThreads { 
            //print("Dispatching thread \(i) ")
            let group = i / threadsPerGroup
            let thread = Thread {
                try? self.doInserts(group, insertsPerThread)
                semaphore.signal()
            }
            thread.start()
        }
        // Esperar a que todos los threads terminen
        for _ in 0..<numberOfThreads {
            semaphore.wait()
        }
        try checkSameSchemaInserts(numberOfGroups, threadsPerGroup, insertsPerThread)
    }

     func sameSchemaInsert_DispatchQueue(numberOfGroups: Int, threadsPerGroup: Int, insertsPerThread: Int) throws{
        try createSchemas(numberOfSchemes: numberOfGroups)
        
        let numberOfThreads = numberOfGroups * threadsPerGroup
        let dispatchGroup = DispatchGroup()
        for i in 0..<numberOfThreads {
            let group = i / threadsPerGroup
            dispatchGroup.enter() 
            //print("Dispatching thread \(i) ")
            DispatchQueue.global().async {
                try? self.doInserts(group, insertsPerThread)
                dispatchGroup.leave() 
            }
        }
        dispatchGroup.wait()
        try checkSameSchemaInserts(numberOfGroups, threadsPerGroup, insertsPerThread)
    }

    func testSameSchema_15x5_Threads() throws{
        try XCTSkipIf(SkipTestSuite, "Life Test Suite not enabled")
        try sameSchemaInsert_Threads(numberOfGroups: 15, threadsPerGroup: 5, insertsPerThread: 50)
    }
    func testSameSchema_40x10_Threads() throws{
        try XCTSkipIf(SkipTestSuite, "Life Test Suite not enabled")
        try sameSchemaInsert_Threads(numberOfGroups: 40, threadsPerGroup: 10, insertsPerThread: 30)
    }
    func testSameSchema_10x40_Threads() throws{
        try XCTSkipIf(SkipTestSuite, "Life Test Suite not enabled")
        try sameSchemaInsert_Threads(numberOfGroups: 10, threadsPerGroup: 40, insertsPerThread: 30)
    }

    func testSameSchema_15x5_DispatchQ() throws{
        try XCTSkipIf(SkipTestSuite, "Life Test Suite not enabled")
        try sameSchemaInsert_DispatchQueue(numberOfGroups: 15, threadsPerGroup: 5, insertsPerThread: 50)
    }
    func testSameSchema_40x10_DispatchQ() throws{
        try XCTSkipIf(SkipTestSuite, "Life Test Suite not enabled")
        try sameSchemaInsert_DispatchQueue(numberOfGroups: 40, threadsPerGroup: 10, insertsPerThread: 30)
    }
    func testSameSchema_10x40_DispatchQ() throws{
        try XCTSkipIf(SkipTestSuite, "Life Test Suite not enabled")
        try sameSchemaInsert_DispatchQueue(numberOfGroups: 10, threadsPerGroup: 40, insertsPerThread: 30)
    }


}

