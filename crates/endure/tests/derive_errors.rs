//! Invalid derive input fails to compile with a message at the offending attribute

#[test]
fn invalid_attributes_are_compile_errors() {
    let cases = trybuild::TestCases::new();
    cases.compile_fail("tests/ui/*.rs");
}
