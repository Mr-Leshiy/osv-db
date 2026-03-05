mod affected;
mod credit;
mod ecosystem;
mod modified_record;
mod package;
mod range;
mod reference;
mod severity;

pub use affected::Affected;
use chrono::{DateTime, Utc};
pub use credit::{Credit, CreditType};
pub use ecosystem::{Ecosystem, EcosystemWithSuffix};
pub use modified_record::OsvModifiedRecord;
pub use package::Package;
pub use range::{Event, Range, RangeType};
pub use reference::{Reference, ReferenceType};
use serde::Deserialize;
use serde_json::Value;
pub use severity::{Severity, SeverityType};

pub type OsvRecordId = String;

/// Root OSV vulnerability record.
///
/// See <https://ossf.github.io/osv-schema/> for the full specification.
#[derive(Debug, Clone, Deserialize)]
pub struct OsvRecord {
    /// Unique vulnerability identifier (e.g. `RUSTSEC-2024-0001`).
    pub id: OsvRecordId,
    /// ISO 8601 timestamp of the last modification.
    pub modified: DateTime<Utc>,
    /// Schema version string.
    pub schema_version: Option<String>,
    /// ISO 8601 timestamp when the record was first published.
    pub published: Option<DateTime<Utc>>,
    /// ISO 8601 timestamp when the record was withdrawn, if applicable.
    pub withdrawn: Option<DateTime<Utc>>,
    /// Alternative identifiers (e.g. CVE IDs) for the same vulnerability.
    pub aliases: Option<Vec<String>>,
    /// Related vulnerability IDs that are not direct aliases.
    pub related: Option<Vec<String>>,
    /// Upstream vulnerability references.
    pub upstream: Option<Vec<String>>,
    /// Brief, one-line description of the vulnerability.
    pub summary: Option<String>,
    /// Full description of the vulnerability (may use Markdown).
    pub details: Option<String>,
    /// Severity ratings at the root level.
    ///
    /// When present, per-package severity in [`Affected`] must be `null`.
    pub severity: Option<Vec<Severity>>,
    /// Packages and version ranges affected by this vulnerability.
    pub affected: Option<Vec<Affected>>,
    /// External references (advisories, fixes, articles, etc.).
    pub references: Option<Vec<Reference>>,
    /// Credits for people or organizations involved in the report.
    pub credits: Option<Vec<Credit>>,
    /// Arbitrary database-specific data.
    pub database_specific: Option<Value>,
}
