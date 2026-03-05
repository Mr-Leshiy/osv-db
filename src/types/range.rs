use serde::Deserialize;
use serde_json::Value;

/// A version range describing when a package is vulnerable.
#[derive(Debug, Clone, Deserialize)]
pub struct Range {
    /// The versioning scheme used for [`Range::events`].
    #[serde(rename = "type")]
    pub range_type: RangeType,
    /// Repository URL — required when `range_type` is [`RangeType::GIT`].
    pub repo: Option<String>,
    /// Ordered list of version events that define the affected range.
    ///
    /// Must contain at least one [`Event::Introduced`] entry.
    pub events: Vec<Event>,
    /// Database-specific additional data.
    pub database_specific: Option<Value>,
}

/// Versioning scheme for a [`Range`].
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub enum RangeType {
    /// Git commit hashes (full 40- or 64-character hex strings, or `"0"`).
    GIT,
    /// Semantic versioning (<https://semver.org/>).
    SEMVER,
    /// Ecosystem-specific versioning (e.g. Maven, `PyPI`).
    ECOSYSTEM,
}

/// A version event that bounds an affected [`Range`].
///
/// Each variant is deserialised from a JSON object with a single key, matching
/// the OSV `oneOf` constraint.
#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
pub enum Event {
    /// The (inclusive) version at which the vulnerability was introduced.
    Introduced {
        /// Version string or commit hash at which the vulnerability was introduced.
        introduced: String,
    },
    /// The (exclusive) version at which the vulnerability was fixed.
    Fixed {
        /// Version string or commit hash at which the fix was released.
        fixed: String,
    },
    /// The last (inclusive) version that is affected.
    ///
    /// Mutually exclusive with [`Event::Fixed`] within the same range.
    LastAffected {
        /// Version string or commit hash of the last affected version.
        last_affected: String,
    },
    /// An exclusive upper bound that limits the range regardless of other events.
    Limit {
        /// Version string or commit hash acting as the upper limit.
        limit: String,
    },
}
