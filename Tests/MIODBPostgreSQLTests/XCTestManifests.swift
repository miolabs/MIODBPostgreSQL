import XCTest

#if !canImport(ObjectiveC)
public func allTests() -> [XCTestCaseEntry] {
    return [
        testCase(MIODBPostgreSQLTests.allTests),
        testCase(CopyEscapeTests.allTests),
    ]
}
#endif
