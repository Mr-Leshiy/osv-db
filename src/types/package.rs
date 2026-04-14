use serde::{Deserialize, Serialize};

use crate::types::EcosystemWithSuffix;

pub type PackageName = String;

/// Identity of an affected package within its ecosystem.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Package {
    /// Ecosystem name, optionally with a suffix (e.g. `"Debian:10"`).
    pub ecosystem: EcosystemWithSuffix,
    /// Package name as used within the ecosystem.
    pub name: PackageName,
    /// Optional Package URL (<https://github.com/package-url/purl-spec>).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub purl: Option<String>,
}
