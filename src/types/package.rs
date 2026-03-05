use serde::Deserialize;

use crate::types::EcosystemWithSuffix;

/// Identity of an affected package within its ecosystem.
#[derive(Debug, Clone, Deserialize)]
pub struct Package {
    /// Ecosystem name, optionally with a suffix (e.g. `"Debian:10"`).
    pub ecosystem: EcosystemWithSuffix,
    /// Package name as used within the ecosystem.
    pub name: String,
    /// Optional Package URL (<https://github.com/package-url/purl-spec>).
    pub purl: Option<String>,
}
