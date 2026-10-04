//! Writes `fuzz/corpus/<target>/<seed>` for every seed (see
//! `animus_fuzz::seeds`). Run via `fuzz/seed-corpus.sh`; the output directory
//! is git-ignored, so no fixture bytes are ever checked in twice.

use std::path::Path;

fn main() {
    let out_root = Path::new(env!("CARGO_MANIFEST_DIR")).join("corpus");
    let mut n = 0usize;
    for seed in animus_fuzz::seeds::all() {
        let dir = out_root.join(&seed.target);
        std::fs::create_dir_all(&dir).expect("create corpus dir");
        // A stable, filesystem-safe name derived from the provenance label.
        let name: String = seed
            .label
            .chars()
            .map(|c| {
                if c.is_ascii_alphanumeric() || c == '.' {
                    c
                } else {
                    '_'
                }
            })
            .collect();
        std::fs::write(dir.join(name), &seed.bytes).expect("write seed");
        n += 1;
    }
    println!("wrote {n} seeds under {}", out_root.display());
}
