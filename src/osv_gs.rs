//! OSV google storage URLs

use strum::{Display, EnumString};

const OSV_STORAGE_URL: &str = "https://storage.googleapis.com/osv-vulnerabilities";

/// Represents an OSV ecosystem used for Google Storage API.
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

pub fn osv_archive_url(ecosystem: Option<&OsvGsEcosystem>) -> String {
    match ecosystem {
        Some(ecosystem) => format!("{OSV_STORAGE_URL}/{ecosystem}/all.zip"),
        None => format!("{OSV_STORAGE_URL}/all.zip"),
    }
}

pub fn osv_modified_id_csv_url(ecosystem: Option<&OsvGsEcosystem>) -> String {
    match ecosystem {
        Some(ecosystem) => format!("{OSV_STORAGE_URL}/{ecosystem}/modified_id.csv"),
        None => format!("{OSV_STORAGE_URL}/modified_id.csv"),
    }
}

pub fn osv_record_url(
    ecosystem: Option<&OsvGsEcosystem>,
    record_path: &str,
) -> String {
    match ecosystem {
        Some(ecosystem) => format!("{OSV_STORAGE_URL}/{ecosystem}/{record_path}.json"),
        None => format!("{OSV_STORAGE_URL}/{record_path}.json"),
    }
}
