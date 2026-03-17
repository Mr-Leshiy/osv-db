//! OSV google storage URLs

use std::{collections::HashSet, fmt::Display};

use itertools::Itertools;
use strum::{Display, EnumString};

const OSV_STORAGE_URL: &str = "https://storage.googleapis.com/osv-vulnerabilities";

/// A set of OSV ecosystems to target.
///
/// An empty list means **all** ecosystems. Use the builder methods to restrict
/// to a specific set.
///
/// # Examples
///
/// ```rust
/// use osv_db::{OsvGsEcosystem, OsvGsEcosystems};
///
/// // All ecosystems
/// let all = OsvGsEcosystems::all();
///
/// // Only crates.io and npm
/// let subset = OsvGsEcosystems::all()
///     .add(OsvGsEcosystem::CratesIo)
///     .add(OsvGsEcosystem::Npm);
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct OsvGsEcosystems(HashSet<OsvGsEcosystem>);

/// A single OSV ecosystem used for Google Storage API.
/// See <https://storage.googleapis.com/osv-vulnerabilities/ecosystems.txt>
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Display, EnumString)]
pub enum OsvGsEcosystem {
    #[strum(to_string = "AlmaLinux")]
    AlmaLinux,
    #[strum(to_string = "Alpaquita")]
    Alpaquita,
    #[strum(to_string = "Alpine")]
    Alpine,
    #[strum(to_string = "Android")]
    Android,
    #[strum(to_string = "BellSoft Hardened Containers")]
    BellSoftHardenedContainers,
    #[strum(to_string = "Bitnami")]
    Bitnami,
    #[strum(to_string = "CRAN")]
    Cran,
    #[strum(to_string = "Chainguard")]
    Chainguard,
    #[strum(to_string = "CleanStart")]
    CleanStart,
    #[strum(to_string = "Debian")]
    Debian,
    #[strum(to_string = "Echo")]
    Echo,
    #[strum(to_string = "GHC")]
    Ghc,
    #[strum(to_string = "GIT")]
    Git,
    #[strum(to_string = "GSD")]
    Gsd,
    #[strum(to_string = "GitHub Actions")]
    GitHubActions,
    #[strum(to_string = "Go")]
    Go,
    #[strum(to_string = "Hackage")]
    Hackage,
    #[strum(to_string = "Hex")]
    Hex,
    #[strum(to_string = "Julia")]
    Julia,
    #[strum(to_string = "Linux")]
    Linux,
    #[strum(to_string = "Mageia")]
    Mageia,
    #[strum(to_string = "Maven")]
    Maven,
    #[strum(to_string = "MinimOS")]
    MinimOS,
    #[strum(to_string = "NuGet")]
    NuGet,
    #[strum(to_string = "OSS-Fuzz")]
    OssFuzz,
    #[strum(to_string = "Packagist")]
    Packagist,
    #[strum(to_string = "Pub")]
    Pub,
    #[strum(to_string = "PyPI")]
    PyPI,
    #[strum(to_string = "Red Hat")]
    RedHat,
    #[strum(to_string = "Rocky Linux")]
    RockyLinux,
    #[strum(to_string = "Root")]
    Root,
    #[strum(to_string = "RubyGems")]
    RubyGems,
    #[strum(to_string = "SUSE")]
    Suse,
    #[strum(to_string = "SwiftURL")]
    SwiftURL,
    #[strum(to_string = "UVI")]
    Uvi,
    #[strum(to_string = "Ubuntu")]
    Ubuntu,
    #[strum(to_string = "VSCode")]
    VSCode,
    #[strum(to_string = "Wolfi")]
    Wolfi,
    #[strum(to_string = "crates.io")]
    CratesIo,
    #[strum(to_string = "npm")]
    Npm,
    #[strum(to_string = "opam")]
    Opam,
    #[strum(to_string = "openEuler")]
    OpenEuler,
    #[strum(to_string = "openSUSE")]
    OpenSUSE,
}

impl Display for OsvGsEcosystems {
    fn fmt(
        &self,
        f: &mut std::fmt::Formatter<'_>,
    ) -> std::fmt::Result {
        if self.is_all() {
            write!(f, "all")
        } else {
            write!(f, "[{}]", self.iter().join(","))
        }
    }
}

impl OsvGsEcosystems {
    /// Creates an empty ecosystem set, meaning **all** ecosystems are targeted.
    #[must_use]
    pub fn all() -> Self {
        Self(HashSet::new())
    }

    /// Returns `true` if no specific ecosystems have been selected, meaning all
    /// ecosystems are targeted.
    #[must_use]
    pub fn is_all(&self) -> bool {
        self.0.is_empty()
    }

    /// Iterates over the explicitly selected ecosystems.
    ///
    /// Returns an empty iterator when [`OsvGsEcosystems::is_all`] is `true`.
    pub fn iter(&self) -> impl Iterator<Item = OsvGsEcosystem> {
        self.0.iter().copied()
    }

    /// Add an [`OsvGsEcosystem`] to the set. Once at least one ecosystem is added,
    /// only the ecosystems explicitly listed are targeted — the implicit "all ecosystems"
    /// behaviour no longer applies.
    #[must_use]
    #[allow(clippy::should_implement_trait)]
    pub fn add(
        mut self,
        ecosystem: OsvGsEcosystem,
    ) -> Self {
        self.0.insert(ecosystem);
        self
    }
}

pub fn osv_archive_url(ecosystem: Option<OsvGsEcosystem>) -> String {
    match ecosystem {
        Some(ecosystem) => format!("{OSV_STORAGE_URL}/{ecosystem}/all.zip"),
        None => format!("{OSV_STORAGE_URL}/all.zip"),
    }
}

pub fn osv_modified_id_csv_url(ecosystem: Option<OsvGsEcosystem>) -> String {
    match ecosystem {
        Some(ecosystem) => format!("{OSV_STORAGE_URL}/{ecosystem}/modified_id.csv"),
        None => format!("{OSV_STORAGE_URL}/modified_id.csv"),
    }
}

pub fn osv_record_url(
    ecosystem: OsvGsEcosystem,
    record_path: &str,
) -> String {
    format!("{OSV_STORAGE_URL}/{ecosystem}/{record_path}.json")
}

#[cfg(test)]
mod tests {
    use super::*;
    use test_case::test_case;

    #[test_case(
        OsvGsEcosystems::all()
        => "all"
    )]
    #[test_case(
        OsvGsEcosystems::all()
        .add(OsvGsEcosystem::Go)
        => "[Go]"
    )]
    #[test_case(
        OsvGsEcosystems::all()
        .add(OsvGsEcosystem::Go)
        .add(OsvGsEcosystem::Npm)
        => "[Go,npm]"
    )]
    #[allow(clippy::needless_pass_by_value)]
    fn osv_gs_ecosystems_display(v: OsvGsEcosystems) -> String {
        v.to_string()
    }
}
