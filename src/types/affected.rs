use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::types::{Package, Range, Severity};

/// A single affected package entry.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Affected {
    /// The affected package identity.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub package: Option<Package>,
    /// Package-level severity (only valid when the root-level severity is absent).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub severity: Vec<Severity>,
    /// Version ranges within which the package is affected.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub ranges: Vec<Range>,
    /// Explicit list of affected version strings.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub versions: Vec<String>,
    /// Ecosystem-specific additional data.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ecosystem_specific: Option<Value>,
    /// Database-specific additional data.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub database_specific: Option<Value>,
}
