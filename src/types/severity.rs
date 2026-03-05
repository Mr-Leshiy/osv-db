use serde::Deserialize;

/// A severity rating expressed in a specific scoring system.
#[derive(Debug, Clone, Deserialize)]
pub struct Severity {
    /// The scoring system used.
    #[serde(rename = "type")]
    pub severity_type: SeverityType,
    /// Score string whose format is defined by [`SeverityType`].
    pub score: String,
}

/// Supported vulnerability severity scoring systems.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub enum SeverityType {
    /// Common Vulnerability Scoring System v2.
    #[serde(rename = "CVSS_V2")]
    CvssV2,
    /// Common Vulnerability Scoring System v3.
    #[serde(rename = "CVSS_V3")]
    CvssV3,
    /// Common Vulnerability Scoring System v4.
    #[serde(rename = "CVSS_V4")]
    CvssV4,
    /// Ubuntu severity levels (`negligible`, `low`, `medium`, `high`, `critical`).
    Ubuntu,
}
