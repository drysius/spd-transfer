//! Relative paths that cannot escape their root.
//!
//! A [`SafeRelPath`] can only be built through validation, so a function taking one does
//! not have to trust its caller or remember to sanitise anything. That is the whole point:
//! the previous project joined a peer-supplied string straight onto the output directory,
//! and no amount of care downstream could undo it.

use std::path::{Component, Path, PathBuf};

use crate::safety::limits::Limits;

/// Names Windows refuses to use as a file name, with or without an extension.
///
/// Writing to one of these opens a device instead of a file, so they are rejected on every
/// platform: a folder received on Linux should still be usable after being copied to a
/// Windows machine.
const WINDOWS_RESERVED: [&str; 22] = [
    "CON", "PRN", "AUX", "NUL", "COM1", "COM2", "COM3", "COM4", "COM5", "COM6", "COM7", "COM8",
    "COM9", "LPT1", "LPT2", "LPT3", "LPT4", "LPT5", "LPT6", "LPT7", "LPT8", "LPT9",
];

/// Characters Windows forbids in a file name. Rejected everywhere for the same reason as
/// the reserved names.
const WINDOWS_FORBIDDEN: [char; 7] = ['<', '>', ':', '"', '|', '?', '*'];

/// Which names a session is willing to carry.
///
/// The strict rules above exist so a folder received on Linux is still usable after being
/// copied to Windows. That is the right default and the wrong answer for a tree that will
/// never leave Unix: a game server directory containing a file called `?` is not a folder
/// anyone can rename, and refusing to carry it means the backup silently misses files.
///
/// So it is a choice, made once per session and only ever loosened when *both* sides say
/// their filesystem can hold such a name - see [`Features::POSIX_NAMES`]. A Windows
/// receiver never says that, so a name it could not write never reaches it.
///
/// [`Features::POSIX_NAMES`]: crate::proto::version::Features::POSIX_NAMES
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum NamePolicy {
    /// Only names every supported platform can write. The default.
    #[default]
    Portable,

    /// Also names that are ordinary on Unix and impossible on Windows: `<>:"|?*`, a
    /// trailing dot or space, and the reserved device names.
    ///
    /// The rules that keep a path from escaping its root - `..`, separators, control
    /// characters, the depth and length limits - are not part of this and still apply.
    Posix,
}

impl NamePolicy {
    /// Whether the Windows-specific rules are enforced.
    pub const fn rejects_windows_traps(self) -> bool {
        matches!(self, Self::Portable)
    }
}

/// A validated path relative to a transfer root.
///
/// Held as components rather than a string, so no separator has to be guessed when it
/// crosses between platforms.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SafeRelPath {
    components: Vec<String>,
}

impl SafeRelPath {
    /// Validates path components that came from a peer.
    ///
    /// # Errors
    /// [`PathError`] describing the first rule broken, naming the component that broke it.
    pub fn from_components(parts: &[String], limits: &Limits) -> Result<Self, PathError> {
        if parts.is_empty() {
            return Err(PathError::Empty);
        }

        if parts.len() > limits.max_path_depth {
            return Err(PathError::TooDeep {
                depth: parts.len(),
                max: limits.max_path_depth,
            });
        }

        let total_len = parts.iter().map(String::len).sum::<usize>() + parts.len();
        if total_len > limits.max_path_len_bytes {
            return Err(PathError::TooLong {
                len: total_len,
                max: limits.max_path_len_bytes,
            });
        }

        for part in parts {
            check_component(part, limits.names)?;
        }

        Ok(Self {
            components: parts.to_vec(),
        })
    }

    /// Validates a local relative path, for the sending side.
    ///
    /// # Errors
    /// [`PathError::NotRelative`] if the path is absolute or contains `..`, otherwise
    /// whatever [`Self::from_components`] rejects.
    pub fn from_relative_path(path: &Path, limits: &Limits) -> Result<Self, PathError> {
        let mut parts = Vec::new();

        for component in path.components() {
            match component {
                Component::Normal(part) => {
                    let text = part.to_str().ok_or_else(|| PathError::NotUtf8 {
                        component: part.to_string_lossy().into_owned(),
                    })?;
                    parts.push(text.to_owned());
                }
                _ => {
                    return Err(PathError::NotRelative {
                        path: path.display().to_string(),
                    });
                }
            }
        }

        Self::from_components(&parts, limits)
    }

    /// The components, for putting on the wire.
    pub fn components(&self) -> &[String] {
        &self.components
    }

    /// The file name, which is the last component.
    pub fn file_name(&self) -> &str {
        // INVARIANT: `from_components` rejects an empty component list, so there is always
        // a last element.
        self.components.last().map_or("", String::as_str)
    }

    /// Resolves this path under `root`, guaranteeing the result stays inside it.
    ///
    /// `root` is canonicalised first, so a symlinked or relative root still produces an
    /// absolute answer that can be compared. The components carry no `..`, so the check
    /// that follows is a belt-and-braces assertion rather than the only line of defence.
    ///
    /// # Errors
    /// [`PathError::RootUnavailable`] if `root` cannot be canonicalised - usually because
    /// it does not exist.
    /// [`PathError::Escape`] if the resolved path leaves `root`, which would mean this
    /// module has a bug.
    pub fn resolve_under(&self, root: &Path) -> Result<PathBuf, PathError> {
        let base = root
            .canonicalize()
            .map_err(|source| PathError::RootUnavailable {
                root: root.display().to_string(),
                source,
            })?;

        let mut resolved = base.clone();
        for component in &self.components {
            resolved.push(component);
        }

        if !resolved.starts_with(&base) {
            return Err(PathError::Escape {
                path: self.to_string(),
                root: base.display().to_string(),
            });
        }

        Ok(resolved)
    }
}

/// Always displayed with `/`, whatever the local separator is, so a log line means the
/// same thing on both machines.
impl core::fmt::Display for SafeRelPath {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "{}", self.components.join("/"))
    }
}

fn check_component(part: &str, names: NamePolicy) -> Result<(), PathError> {
    if part.is_empty() {
        return Err(PathError::EmptyComponent);
    }

    if part == "." || part == ".." {
        return Err(PathError::Traversal {
            component: part.to_owned(),
        });
    }

    if part.contains('/') || part.contains('\\') {
        return Err(PathError::Separator {
            component: part.to_owned(),
        });
    }

    if let Some(found) = part.chars().find(|c| c.is_control()) {
        return Err(PathError::ControlCharacter {
            component: part.to_owned(),
            code: found as u32,
        });
    }

    // Everything below is a Windows rule. A session that agreed both ends are on a
    // filesystem without them carries the name as it is.
    if !names.rejects_windows_traps() {
        return Ok(());
    }

    if let Some(found) = part.chars().find(|c| WINDOWS_FORBIDDEN.contains(c)) {
        return Err(PathError::ForbiddenCharacter {
            component: part.to_owned(),
            character: found,
        });
    }

    // Windows silently strips these, so "report.txt " and "report.txt" would collide and
    // the second file would overwrite the first without either side noticing.
    if part.ends_with('.') || part.ends_with(' ') {
        return Err(PathError::TrailingDotOrSpace {
            component: part.to_owned(),
        });
    }

    let stem = part.split('.').next().unwrap_or(part).to_ascii_uppercase();
    if WINDOWS_RESERVED.contains(&stem.as_str()) {
        return Err(PathError::ReservedName {
            component: part.to_owned(),
        });
    }

    Ok(())
}

/// Why a path was refused.
///
/// Every variant names the offending component: a rejected transfer has to say which file
/// caused it, or the user cannot act on the message.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum PathError {
    /// The path had no components at all.
    #[error("rejected path: it is empty")]
    Empty,

    /// One component was an empty string.
    #[error("rejected path: it contains an empty component")]
    EmptyComponent,

    /// A component was `.` or `..`.
    #[error("rejected path: {component:?} component; nothing was written")]
    Traversal {
        /// The offending component.
        component: String,
    },

    /// A component contained a path separator, which would smuggle in extra depth.
    #[error("rejected path: separator inside the component {component:?}; nothing was written")]
    Separator {
        /// The offending component.
        component: String,
    },

    /// A component contained a control character.
    #[error("rejected path: control character U+{code:04X} in {component:?}")]
    ControlCharacter {
        /// The offending component.
        component: String,
        /// The character's code point.
        code: u32,
    },

    /// A component contained a character Windows forbids.
    #[error("rejected path: {character:?} is not allowed in a file name ({component:?})")]
    ForbiddenCharacter {
        /// The offending component.
        component: String,
        /// The character found.
        character: char,
    },

    /// A component ended in a dot or space, which Windows would strip.
    #[error("rejected path: {component:?} ends in a dot or space, which Windows removes")]
    TrailingDotOrSpace {
        /// The offending component.
        component: String,
    },

    /// A component is a Windows device name.
    #[error("rejected path: {component:?} is a reserved device name on Windows")]
    ReservedName {
        /// The offending component.
        component: String,
    },

    /// The path has more components than allowed.
    #[error("rejected path: depth {depth} exceeds the limit of {max} (max_path_depth)")]
    TooDeep {
        /// Depth seen.
        depth: usize,
        /// Configured limit.
        max: usize,
    },

    /// The path is longer than allowed.
    #[error("rejected path: {len} B exceeds the limit of {max} B (max_path_len_bytes)")]
    TooLong {
        /// Length seen.
        len: usize,
        /// Configured limit.
        max: usize,
    },

    /// A local path was absolute or contained `..`.
    #[error("{path} is not a relative path")]
    NotRelative {
        /// The path as given.
        path: String,
    },

    /// A local path component is not valid UTF-8 and cannot cross to another platform.
    #[error("{component:?} is not valid UTF-8, so it cannot be sent")]
    NotUtf8 {
        /// The component, lossily converted for display.
        component: String,
    },

    /// The destination root could not be resolved.
    #[error("destination {root} is unavailable")]
    RootUnavailable {
        /// The root as given.
        root: String,
        /// Underlying filesystem error.
        source: std::io::Error,
    },

    /// The resolved path left the root.
    #[error("rejected path: {path} resolves outside {root}; nothing was written")]
    Escape {
        /// The relative path.
        path: String,
        /// The root it should have stayed under.
        root: String,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parts(values: &[&str]) -> Vec<String> {
        values.iter().map(|value| (*value).to_owned()).collect()
    }

    fn safe(values: &[&str]) -> Result<SafeRelPath, PathError> {
        SafeRelPath::from_components(&parts(values), &Limits::DEFAULT)
    }

    #[test]
    fn an_ordinary_path_is_accepted_and_prints_portably() {
        let path = safe(&["docs", "notes.txt"]).unwrap();

        assert_eq!(path.to_string(), "docs/notes.txt");
        assert_eq!(path.file_name(), "notes.txt");
    }

    #[test]
    fn traversal_components_are_rejected() {
        assert!(matches!(
            safe(&["docs", ".."]).unwrap_err(),
            PathError::Traversal { .. }
        ));
        assert!(matches!(
            safe(&["."]).unwrap_err(),
            PathError::Traversal { .. }
        ));
    }

    #[test]
    fn separators_inside_a_component_are_rejected() {
        assert!(matches!(
            safe(&["docs/../etc"]).unwrap_err(),
            PathError::Separator { .. }
        ));
        assert!(matches!(
            safe(&["docs\\..\\etc"]).unwrap_err(),
            PathError::Separator { .. }
        ));
    }

    #[test]
    fn windows_specific_traps_are_rejected_on_every_platform() {
        assert!(matches!(
            safe(&["CON"]).unwrap_err(),
            PathError::ReservedName { .. }
        ));
        assert!(matches!(
            safe(&["com1.txt"]).unwrap_err(),
            PathError::ReservedName { .. }
        ));
        assert!(matches!(
            safe(&["report.txt "]).unwrap_err(),
            PathError::TrailingDotOrSpace { .. }
        ));
        assert!(matches!(
            safe(&["what?.txt"]).unwrap_err(),
            PathError::ForbiddenCharacter { .. }
        ));
        assert!(matches!(
            safe(&["C:"]).unwrap_err(),
            PathError::ForbiddenCharacter { .. }
        ));
    }

    fn posix(values: &[&str]) -> Result<SafeRelPath, PathError> {
        let limits = Limits {
            names: NamePolicy::Posix,
            ..Limits::DEFAULT
        };
        SafeRelPath::from_components(&parts(values), &limits)
    }

    #[test]
    fn a_posix_session_carries_names_windows_could_not_write() {
        assert_eq!(
            posix(&["?", "README.txt"]).unwrap().to_string(),
            "?/README.txt"
        );
        assert_eq!(posix(&["what?.txt"]).unwrap().file_name(), "what?.txt");
        assert!(posix(&["CON"]).is_ok());
        assert!(posix(&["report.txt "]).is_ok());
        assert!(posix(&["a:b*c"]).is_ok());
    }

    #[test]
    fn a_posix_session_still_cannot_escape_its_root() {
        assert!(matches!(
            posix(&["docs", ".."]).unwrap_err(),
            PathError::Traversal { .. }
        ));
        assert!(matches!(
            posix(&["docs/../etc"]).unwrap_err(),
            PathError::Separator { .. }
        ));
        assert!(matches!(
            posix(&["na\u{0}me"]).unwrap_err(),
            PathError::ControlCharacter { .. }
        ));
        assert!(matches!(
            posix(&[""]).unwrap_err(),
            PathError::EmptyComponent
        ));
    }

    #[test]
    fn control_characters_are_rejected() {
        assert!(matches!(
            safe(&["na\u{0}me"]).unwrap_err(),
            PathError::ControlCharacter { .. }
        ));
    }

    #[test]
    fn limits_bound_depth_and_length() {
        let deep = vec!["a".to_owned(); Limits::DEFAULT.max_path_depth + 1];
        assert!(matches!(
            SafeRelPath::from_components(&deep, &Limits::DEFAULT).unwrap_err(),
            PathError::TooDeep { .. }
        ));

        let long = vec!["a".repeat(Limits::DEFAULT.max_path_len_bytes)];
        assert!(matches!(
            SafeRelPath::from_components(&long, &Limits::DEFAULT).unwrap_err(),
            PathError::TooLong { .. }
        ));
    }

    #[test]
    fn an_absolute_local_path_is_not_relative() {
        let absolute = if cfg!(windows) {
            Path::new(r"C:\windows\system32")
        } else {
            Path::new("/etc/passwd")
        };

        assert!(matches!(
            SafeRelPath::from_relative_path(absolute, &Limits::DEFAULT).unwrap_err(),
            PathError::NotRelative { .. }
        ));
    }

    #[test]
    fn resolution_stays_under_the_root() {
        let root = std::env::temp_dir();
        let resolved = safe(&["nested", "file.bin"])
            .unwrap()
            .resolve_under(&root)
            .unwrap();

        assert!(resolved.starts_with(root.canonicalize().unwrap()));
        assert!(resolved.ends_with("file.bin"));
    }

    #[test]
    fn a_missing_root_is_reported_as_such() {
        let missing = std::env::temp_dir().join("spd-root-that-does-not-exist-42");

        assert!(matches!(
            safe(&["file.bin"])
                .unwrap()
                .resolve_under(&missing)
                .unwrap_err(),
            PathError::RootUnavailable { .. }
        ));
    }
}
