use std::error::Error;
use std::path::{Path, PathBuf, Component};
use clap::ArgMatches;

fn remove_relative_components(path: &Path) -> Result<PathBuf, Box<dyn Error>> {
    let mut components = Vec::new();
    let mut skip = 0;
    let comp_iter = path.components().filter(|comp | match comp {
        Component::CurDir => false,
        _ => true,
    });
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

    Ok(components.iter().rev().collect::<PathBuf>())
}


/// Makes a path absolute, removes '.' and '..' elements, but preserves symlinks
/// The current working directory is taken to be `base_path`
fn to_semi_canonical_path_from(path: &Path, base_path: &Path) -> Result<PathBuf, Box<dyn Error>> {
    let path = remove_relative_components(&path)?;
    if path.is_absolute() {
        return Ok(path)
    }

    Ok(base_path.join(path))
}

/// Makes a path absolute, removes '.' and '..' elements, but preserves symlinks
pub fn to_semi_canonical_path(path: &Path) -> Result<PathBuf, Box<dyn Error>> {
    to_semi_canonical_path_from(path, &std::env::current_dir()?)
}

/// Makes an absolute semi-canonical path from a command line argument
pub fn path_from_arg(args: &ArgMatches<'_>, name: &str) -> Result<PathBuf, Box<dyn Error>> {
    match args.value_of_os(name) {
        Some(raw_path) => to_semi_canonical_path(Path::new(raw_path)),
        _ => Err(From::from(format!("Missing required argument \"{}\"", name))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_to_semi_canonical_path() -> Result<(), Box<dyn Error>> {
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
            let result_path = to_semi_canonical_path_from(&Path::new(relative), base_path)?;
            assert_eq!(result_path.to_string_lossy(), *absolute);
        }
        Ok(())
    }
}