use std::{fmt::Display, str::FromStr};

use serde::{Deserialize, Deserializer, Serialize, de};
use strum::{Display, EnumString};

/// Ecosystem name, optionally with a suffix (e.g. `"Debian:10"`).
#[derive(Debug, Clone)]
pub struct EcosystemWithSuffix(Ecosystem, Option<String>);

/// Represents an OSV ecosystem, as defined by the OSV schema.
/// See <https://github.com/ossf/osv-schema/blob/main/validation/schema.json>
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Display, EnumString)]
pub enum Ecosystem {
    #[strum(to_string = "AlmaLinux")]
    AlmaLinux,
    #[strum(to_string = "Alpaquita")]
    Alpaquita,
    #[strum(to_string = "Alpine")]
    Alpine,
    #[strum(to_string = "Android")]
    Android,
    #[strum(to_string = "Azure Linux")]
    AzureLinux,
    #[strum(to_string = "BellSoft Hardened Containers")]
    BellSoftHardenedContainers,
    #[strum(to_string = "Bioconductor")]
    Bioconductor,
    #[strum(to_string = "Bitnami")]
    Bitnami,
    #[strum(to_string = "Chainguard")]
    Chainguard,
    #[strum(to_string = "CleanStart")]
    CleanStart,
    #[strum(to_string = "ConanCenter")]
    ConanCenter,
    #[strum(to_string = "CRAN")]
    Cran,
    #[strum(to_string = "crates.io")]
    CratesIo,
    #[strum(to_string = "Debian")]
    Debian,
    #[strum(to_string = "Docker Hardened Images")]
    DockerHardenedImages,
    #[strum(to_string = "Echo")]
    Echo,
    #[strum(to_string = "FreeBSD")]
    FreeBSD,
    #[strum(to_string = "GHC")]
    Ghc,
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
    #[strum(to_string = "Kubernetes")]
    Kubernetes,
    #[strum(to_string = "Linux")]
    Linux,
    #[strum(to_string = "Mageia")]
    Mageia,
    #[strum(to_string = "Maven")]
    Maven,
    #[strum(to_string = "MinimOS")]
    MinimOS,
    #[strum(to_string = "npm")]
    Npm,
    #[strum(to_string = "NuGet")]
    NuGet,
    #[strum(to_string = "opam")]
    Opam,
    #[strum(to_string = "openEuler")]
    OpenEuler,
    #[strum(to_string = "openSUSE")]
    OpenSUSE,
    #[strum(to_string = "OSS-Fuzz")]
    OssFuzz,
    #[strum(to_string = "Packagist")]
    Packagist,
    #[strum(to_string = "Photon OS")]
    PhotonOS,
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
    #[strum(to_string = "Ubuntu")]
    Ubuntu,
    #[strum(to_string = "VSCode")]
    VSCode,
    #[strum(to_string = "Wolfi")]
    Wolfi,
    #[strum(to_string = "GIT")]
    Git,
}

impl EcosystemWithSuffix {
    /// Returns the [`Ecosystem`] variant, without the suffix.
    #[must_use]
    pub fn ecosystem(&self) -> Ecosystem {
        self.0
    }

    /// Returns the optional suffix component (e.g. `"10"` for `"Debian:10"`),
    /// or `None` if no suffix is present.
    #[must_use]
    pub fn suffix(&self) -> Option<&str> {
        self.1.as_deref()
    }
}

impl Display for EcosystemWithSuffix {
    fn fmt(
        &self,
        f: &mut std::fmt::Formatter<'_>,
    ) -> std::fmt::Result {
        write!(f, "{}", self.0)?;
        if let Some(suffix) = &self.1 {
            write!(f, ":{suffix}")?;
        }
        Ok(())
    }
}

impl FromStr for EcosystemWithSuffix {
    type Err = <Ecosystem as FromStr>::Err;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if let Some((s, suffix)) = s.split_once(':') {
            Ok(Self(s.parse()?, Some(suffix.to_string())))
        } else {
            Ok(Self(s.parse()?, None))
        }
    }
}

impl<'de> Deserialize<'de> for EcosystemWithSuffix {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let s = String::deserialize(deserializer)?;
        s.parse().map_err(|_| de::Error::unknown_variant(&s, &[]))
    }
}

impl Serialize for EcosystemWithSuffix {
    fn serialize<S>(
        &self,
        serializer: S,
    ) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.to_string().serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for Ecosystem {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let s = String::deserialize(deserializer)?;
        s.parse().map_err(|_| de::Error::unknown_variant(&s, &[]))
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;
    use test_case::test_case;

    use super::*;

    // Roundtrip: Display produces the canonical OSV string, parse recovers the variant.
    #[test_case("AlmaLinux", Ecosystem::AlmaLinux)]
    #[test_case("Alpaquita", Ecosystem::Alpaquita)]
    #[test_case("Alpine", Ecosystem::Alpine)]
    #[test_case("Android", Ecosystem::Android)]
    #[test_case("Azure Linux", Ecosystem::AzureLinux)]
    #[test_case("BellSoft Hardened Containers", Ecosystem::BellSoftHardenedContainers)]
    #[test_case("Bioconductor", Ecosystem::Bioconductor)]
    #[test_case("Bitnami", Ecosystem::Bitnami)]
    #[test_case("Chainguard", Ecosystem::Chainguard)]
    #[test_case("CleanStart", Ecosystem::CleanStart)]
    #[test_case("ConanCenter", Ecosystem::ConanCenter)]
    #[test_case("CRAN", Ecosystem::Cran)]
    #[test_case("crates.io", Ecosystem::CratesIo)]
    #[test_case("Debian", Ecosystem::Debian)]
    #[test_case("Docker Hardened Images", Ecosystem::DockerHardenedImages)]
    #[test_case("Echo", Ecosystem::Echo)]
    #[test_case("FreeBSD", Ecosystem::FreeBSD)]
    #[test_case("GHC", Ecosystem::Ghc)]
    #[test_case("GitHub Actions", Ecosystem::GitHubActions)]
    #[test_case("Go", Ecosystem::Go)]
    #[test_case("Hackage", Ecosystem::Hackage)]
    #[test_case("Hex", Ecosystem::Hex)]
    #[test_case("Julia", Ecosystem::Julia)]
    #[test_case("Kubernetes", Ecosystem::Kubernetes)]
    #[test_case("Linux", Ecosystem::Linux)]
    #[test_case("Mageia", Ecosystem::Mageia)]
    #[test_case("Maven", Ecosystem::Maven)]
    #[test_case("MinimOS", Ecosystem::MinimOS)]
    #[test_case("npm", Ecosystem::Npm)]
    #[test_case("NuGet", Ecosystem::NuGet)]
    #[test_case("opam", Ecosystem::Opam)]
    #[test_case("openEuler", Ecosystem::OpenEuler)]
    #[test_case("openSUSE", Ecosystem::OpenSUSE)]
    #[test_case("OSS-Fuzz", Ecosystem::OssFuzz)]
    #[test_case("Packagist", Ecosystem::Packagist)]
    #[test_case("Photon OS", Ecosystem::PhotonOS)]
    #[test_case("Pub", Ecosystem::Pub)]
    #[test_case("PyPI", Ecosystem::PyPI)]
    #[test_case("Red Hat", Ecosystem::RedHat)]
    #[test_case("Rocky Linux", Ecosystem::RockyLinux)]
    #[test_case("Root", Ecosystem::Root)]
    #[test_case("RubyGems", Ecosystem::RubyGems)]
    #[test_case("SUSE", Ecosystem::Suse)]
    #[test_case("SwiftURL", Ecosystem::SwiftURL)]
    #[test_case("Ubuntu", Ecosystem::Ubuntu)]
    #[test_case("VSCode", Ecosystem::VSCode)]
    #[test_case("Wolfi", Ecosystem::Wolfi)]
    #[test_case("GIT", Ecosystem::Git)]
    #[allow(clippy::needless_pass_by_value)]
    fn display_and_parse_roundtrip(
        osv_string: &str,
        expected: Ecosystem,
    ) {
        let eco_from_json: Ecosystem = serde_json::from_value(json!(osv_string)).unwrap();
        let eco_from_str: Ecosystem = osv_string.parse().unwrap();

        assert_eq!(expected, eco_from_str);
        assert_eq!(expected, eco_from_json);
        assert_eq!(expected.to_string(), osv_string);

        let ews_from_str: EcosystemWithSuffix = osv_string.parse().unwrap();
        let ews_from_json: EcosystemWithSuffix = serde_json::from_value(json!(osv_string)).unwrap();
        assert_eq!(ews_from_str.to_string(), osv_string);
        assert_eq!(ews_from_json.to_string(), osv_string);
    }
}
