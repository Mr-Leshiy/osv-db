mod affected;
mod credit;
mod ecosystem;
mod modified_record;
mod package;
mod range;
mod reference;
mod severity;
#[cfg(test)]
mod tests;

pub use affected::Affected;
use chrono::{DateTime, Utc};
pub use credit::{Credit, CreditType};
pub use ecosystem::{Ecosystem, EcosystemWithSuffix};
pub use modified_record::OsvModifiedRecord;
pub use package::{Package, PackageName};
pub use range::{Event, Range, RangeType};
pub use reference::{Reference, ReferenceType};
use serde::{Deserialize, Serialize};
use serde_json::Value;
pub use severity::{Severity, SeverityType};

pub type OsvRecordId = String;

/// Root OSV vulnerability record.
///
/// See <https://ossf.github.io/osv-schema/> for the full specification.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct OsvRecord {
    /// Unique vulnerability identifier (e.g. `RUSTSEC-2024-0001`).
    pub id: OsvRecordId,
    /// ISO 8601 timestamp of the last modification.
    pub modified: DateTime<Utc>,
    /// Schema version string.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schema_version: Option<String>,
    /// ISO 8601 timestamp when the record was first published.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub published: Option<DateTime<Utc>>,
    /// ISO 8601 timestamp when the record was withdrawn, if applicable.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub withdrawn: Option<DateTime<Utc>>,
    /// Alternative identifiers (e.g. CVE IDs) for the same vulnerability.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub aliases: Vec<String>,
    /// Related vulnerability IDs that are not direct aliases.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub related: Vec<String>,
    /// Upstream vulnerability references.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub upstream: Vec<String>,
    /// Brief, one-line description of the vulnerability.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub summary: Option<String>,
    /// Full description of the vulnerability (may use Markdown).
    pub details: Option<String>,
    /// Severity ratings at the root level.
    ///
    /// When present, per-package severity in [`crate::types::Affected`] must be `null`.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub severity: Vec<Severity>,
    /// Packages and version ranges affected by this vulnerability.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub affected: Vec<Affected>,
    /// External references (advisories, fixes, articles, etc.).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub references: Vec<Reference>,
    /// Credits for people or organizations involved in the report.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub credits: Vec<Credit>,
    /// Arbitrary database-specific data.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub database_specific: Option<Value>,
}
