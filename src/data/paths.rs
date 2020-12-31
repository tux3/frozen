use clap::ArgMatches;
use eyre::{eyre, Result};
use std::ffi::OsStr;
#[cfg(unix)]
use std::os::unix::ffi::OsStrExt;
use std::path::{Component, Path, PathBuf};

fn remove_relative_components(path: &Path) -> PathBuf {
    let mut components = Vec::new();
    let mut skip = 0;
    let comp_iter = path.components().filter(|comp| !matches!(comp, Component::CurDir));
    for comp in comp_iter.rev() {
        if let Component::ParentDir = comp {
            skip += 1;
        } else if let Component::RootDir = comp {
            components.push(Component::RootDir)
        } else if skip > 0 {
            skip -= 1;
        } else {
            components.push(comp);
        }
    }

    components.iter().rev().collect::<PathBuf>()
}

/// Makes a path absolute, removes '.' and '..' elements, but preserves symlinks
/// The current working directory is taken to be `base_path`
fn to_semi_canonical_path_from(path: &Path, base_path: &Path) -> PathBuf {
    let path = remove_relative_components(&path);
    if path.is_absolute() {
        return path;
    }

    base_path.join(path)
}

/// Makes a path absolute, removes '.' and '..' elements, but preserves symlinks
pub fn to_semi_canonical_path(path: &Path) -> Result<PathBuf> {
    Ok(to_semi_canonical_path_from(path, &std::env::current_dir()?))
}

/// Makes an absolute semi-canonical path from a command line argument
pub fn path_from_arg(args: &ArgMatches<'_>, name: &str) -> Result<PathBuf> {
    match args.value_of_os(name) {
        Some(raw_path) => to_semi_canonical_path(Path::new(raw_path)),
        _ => Err(eyre!("Missing required argument \"{}\"", name)),
    }
}

#[cfg(unix)]
pub fn path_to_bytes(path: &Path) -> Result<&[u8]> {
    let os_str = path.as_os_str();
    Ok(os_str.as_bytes())
}

#[cfg(unix)]
pub fn filename_to_bytes(path: &Path) -> Result<&[u8]> {
    let os_str = path.file_name().unwrap();
    Ok(os_str.as_bytes())
}

#[cfg(unix)]
pub fn path_from_bytes(bytes: &[u8]) -> Result<&Path> {
    let os_str = OsStr::from_bytes(bytes);
    Ok(Path::new(os_str))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_to_semi_canonical_path() -> Result<()> {
        let base_path = Path::new("/base/path/");
        let tests_paths = [
            ("/", "/"),
            ("/a/b/", "/a/b"),
            ("/../.../.././...", "/..."),
            ("test", "/base/path/test"),
            ("./test", "/base/path/test"),
            ("./test/", "/base/path/test"),
            ("./test/./", "/base/path/test"),
            ("a/b1/../b2/./c", "/base/path/a/b2/c"),
            ("a/b/c/d/./././../../.././x", "/base/path/a/x"),
        ];

        for (relative, absolute) in tests_paths.iter() {
            let result_path = to_semi_canonical_path_from(&Path::new(relative), base_path);
            assert_eq!(result_path.to_string_lossy(), *absolute);
        }
        Ok(())
    }

    #[test]
    fn path_bytes_roundtrip() -> Result<()> {
        let path = Path::new("/some/ÚTF-8/path\\somewhere 😁");
        let to_bytes = path_to_bytes(path)?;
        let from_bytes = path_from_bytes(to_bytes)?;
        assert_eq!(path, from_bytes);
        assert_eq!(to_bytes, path_to_bytes(from_bytes)?);
        Ok(())
    }
}
