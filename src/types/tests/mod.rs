use test_case::test_case;

use super::OsvRecord;


#[test_case("GHSA-8rf9-c59g-f82f.json")]
#[test_case("MAL-2026-1429.json")]
#[test_case("RUSTSEC-2025-0112.json")]
#[tokio::test]
async fn json_serde_roundtrip(path: &str) {
    const VERSION: &str = "v1.7.5";

    let record_file = std::fs::File::open(format!("src/types/tests/{path}")).unwrap();
    let record: OsvRecord = serde_json::from_reader(record_file).unwrap();
    let record_json = serde_json::to_value(record).unwrap();
    
    let record_schema_resp = reqwest::get(format!("https://raw.githubusercontent.com/ossf/osv-schema/refs/tags/{VERSION}/validation/schema.json")).await.unwrap();
    let record_schema_bytes = record_schema_resp.bytes().await.unwrap();
    let record_schema_json: serde_json::Value = serde_json::from_slice(&record_schema_bytes).unwrap();

    assert!(jsonschema::is_valid(&record_schema_json, &record_json));
}