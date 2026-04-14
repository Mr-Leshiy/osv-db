use serde::{Deserialize, Serialize};

/// An external reference for the vulnerability.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Reference {
    /// Classification of this reference.
    #[serde(rename = "type")]
    pub reference_type: ReferenceType,
    /// URI of the reference.
    pub url: String,
}

/// Classification of an external [`Reference`].
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub enum ReferenceType {
    /// A published security advisory.
    ADVISORY,
    /// An article or blog post.
    ARTICLE,
    /// A tool or method for detecting the vulnerability.
    DETECTION,
    /// A discussion thread (e.g. GitHub issue, mailing list).
    DISCUSSION,
    /// The original vulnerability report.
    REPORT,
    /// A patch or commit that fixes the vulnerability.
    FIX,
    /// A patch or commit that introduced the vulnerability.
    INTRODUCED,
    /// A git commit or tag (generic).
    GIT,
    /// The package in a registry.
    PACKAGE,
    /// Evidence supporting the existence of the vulnerability.
    EVIDENCE,
    /// Any other web resource.
    WEB,
}
