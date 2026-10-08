//! Real-filesystem helpers for the `chaos_disk_full` scenario (issue #1221):
//! size-limited tmpfs mounts and a ballast file that fills one to ENOSPC.
//!
//! A mount needs `CAP_SYS_ADMIN`. Everything here runs the mount through
//! `sudo -n` when the test is not already root, and [`mount_supported`] lets
//! the scenario skip cleanly (with the reason) where mounting is not allowed.
#![allow(dead_code, reason = "used by the chaos target, not the soak target")]

use std::fs::OpenOptions;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::Command;

/// Run `prog args...` with privilege (directly as root, else `sudo -n`).
fn privileged(prog: &str, args: &[&str]) -> Result<(), String> {
    let is_root = Command::new("id")
        .arg("-u")
        .output()
        .map(|o| String::from_utf8_lossy(&o.stdout).trim() == "0")
        .unwrap_or(false);
    let mut cmd = if is_root {
        Command::new(prog)
    } else {
        let mut c = Command::new("sudo");
        c.arg("-n").arg(prog);
        c
    };
    let out = cmd
        .args(args)
        .output()
        .map_err(|e| format!("cannot run `{prog}`: {e}"))?;
    if out.status.success() {
        Ok(())
    } else {
        Err(format!(
            "`{prog} {}` failed ({}): {}",
            args.join(" "),
            out.status,
            String::from_utf8_lossy(&out.stderr).trim()
        ))
    }
}

/// A size-limited tmpfs mounted at `path`; unmounted on drop.
pub struct Tmpfs {
    path: PathBuf,
}

impl Tmpfs {
    pub fn mount(path: &Path, size_mb: u64) -> Result<Self, String> {
        std::fs::create_dir_all(path).map_err(|e| format!("mkdir {}: {e}", path.display()))?;
        let opts = format!("size={size_mb}m,mode=1777");
        privileged(
            "mount",
            &[
                "-t",
                "tmpfs",
                "-o",
                &opts,
                "animus-chaos",
                &path.to_string_lossy(),
            ],
        )?;
        Ok(Self {
            path: path.to_path_buf(),
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for Tmpfs {
    fn drop(&mut self) {
        let p = self.path.to_string_lossy().into_owned();
        // A node that is still shutting down can hold the mount busy for a
        // moment; fall back to a lazy unmount rather than leak the mount.
        if privileged("umount", &[&p]).is_err() {
            let _ = privileged("umount", &["-l", &p]);
        }
    }
}

/// `Ok(())` when this process can mount a tmpfs (probed with a 1 MiB mount
/// under `scratch`), else the reason, for a clean skip.
pub fn mount_supported(scratch: &Path) -> Result<(), String> {
    let probe = scratch.join("mount-probe");
    let m = Tmpfs::mount(&probe, 1)?;
    drop(m);
    Ok(())
}

/// Append zeros to `dir/ballast` until the filesystem refuses (ENOSPC), so
/// the mount has essentially no free space left. Returns the file's size.
/// Calling it again tops the file up with whatever space has appeared since
/// (a node that deletes a file of its own would otherwise get it back).
pub fn fill(dir: &Path) -> std::io::Result<u64> {
    let path = dir.join("ballast");
    let mut f = OpenOptions::new().create(true).append(true).open(&path)?;
    for chunk in [256 * 1024usize, 16 * 1024, 4 * 1024, 512] {
        let buf = vec![0u8; chunk];
        while f.write_all(&buf).is_ok() {}
    }
    drop(f);
    Ok(std::fs::metadata(&path)?.len())
}

/// Delete the ballast, returning its space to the filesystem.
pub fn free(dir: &Path) {
    let _ = std::fs::remove_file(dir.join("ballast"));
}
