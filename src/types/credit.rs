use serde::{Deserialize, Serialize};

/// A credit entry for a person or organization.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Credit {
    /// Name of the credited person or organization.
    pub name: String,
    /// Contact URIs or handles for the credited party.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub contact: Vec<String>,
    /// The role this party played.
    #[serde(rename = "type", skip_serializing_if = "Option::is_none")]
    pub credit_type: Option<CreditType>,
}

/// The role a credited party played in discovering or fixing the vulnerability.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub enum CreditType {
    /// Discovered the vulnerability.
    FINDER,
    /// Reported the vulnerability to the affected vendor.
    REPORTER,
    /// Analyzed the vulnerability.
    ANALYST,
    /// Coordinated the disclosure.
    COORDINATOR,
    /// Developed the remediation.
    #[serde(rename = "REMEDIATION_DEVELOPER")]
    RemediationDeveloper,
    /// Reviewed the remediation.
    #[serde(rename = "REMEDIATION_REVIEWER")]
    RemediationReviewer,
    /// Verified the remediation.
    #[serde(rename = "REMEDIATION_VERIFIER")]
    RemediationVerifier,
    /// A tool used in the process.
    TOOL,
    /// Sponsored the work.
    SPONSOR,
    /// Any other role.
    OTHER,
}
