use serde::Deserialize;
use serde_json::Value;

use crate::types::{Package, Range, Severity};

/// A single affected package entry.
#[derive(Debug, Clone, Deserialize)]
pub struct Affected {
    /// The affected package identity.
    pub package: Option<Package>,
    /// Package-level severity (only valid when the root-level severity is absent).
    #[serde(default)]
    pub severity: Vec<Severity>,
    /// Version ranges within which the package is affected.
    #[serde(default)]
    pub ranges: Vec<Range>,
    /// Explicit list of affected version strings.
    #[serde(default)]
    pub versions: Vec<String>,
    /// Ecosystem-specific additional data.
    pub ecosystem_specific: Option<Value>,
    /// Database-specific additional data.
    pub database_specific: Option<Value>,
}
