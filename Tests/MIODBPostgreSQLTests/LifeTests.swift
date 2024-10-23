
import XCTest
@testable import MIODB
@testable import MIODBPostgreSQL
import CLibPQ

// ----------------------------------
// Life tests for postgreSQL
// ----------------------------------
//
// Some tests to check actual behaviour of the libraries
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

final class LifeTests: XCTestCase {
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

    private var schemas: [String] = []
    private var threadIds: [Int64] = []
    private let mutex = DispatchSemaphore(value: 1)
    private let uuidBut4 = "00000000-0000-0000-0000-00000001"
    private let schemaBut4 = "_0000000000000000000000000001"

    override class func setUp() {
        conn01 = MDBPostgreConnection(host: LifeTests.host, port: LifeTests.port01, user: LifeTests.user, password: LifeTests.pass)
        do {
            inst01 = try conn01?.create(db01) as? MIODBPostgreSQL
            //try LifeTests.inst!.dropCollection(LifeTests.testCollection)

        } catch {
            XCTFail("Error setting up test suite: cant connect to postgreSQL database")
        }
        super.setUp()
    }

    override class func tearDown() {
        LifeTests.inst01?.disconnect()
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
        guard let db = LifeTests.inst01 else {
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

// MARK: - single quote    
    // MIODB automatically escapes single quotes and we receive one quote in the query. All as expected
    func testSingleQuote() throws{
        try createSchemas(numberOfSchemes: 1)
        let schema = getSchemaString(0)
        let queryInsert = try MDBQuery( schema + ".tabla1") {
            Insert( [
                "id": UUID().uuidString,
                "nombre": "Nombre con ' comilla"
            ] )
        }
        XCTAssertEqual(queryInsert.values["nombre"]?.value, "'Nombre con '' comilla'")
        try LifeTests.inst01!.executeQueryString(MDBQueryEncoderSQL(queryInsert).rawQuery())
        let rows = try LifeTests.inst01!.executeQueryString(MDBQueryEncoderSQL(MDBQuery( schema + ".tabla1").select()).rawQuery())
        XCTAssertEqual(rows!.count, 1)
        XCTAssertEqual(rows![0]["nombre"] as! String, "Nombre con ' comilla")
    }





}

