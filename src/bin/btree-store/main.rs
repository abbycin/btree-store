//! `btree-store` — offline database maintenance CLI.
//!
//! `migrate` rebuilds a database in the current on-disk format as a separate
//! file. `check` performs a read-only integrity check of a static v2 file.
//! `compact` rebuilds a static v2 file into the destination it is given, replacing whatever
//! is there and never replacing the source; with `--info` it reads only the source
//! and reports what that copy would cost.

mod check;
mod cli;
mod compact;
mod migrate;
mod staging;
mod v1;

fn main() {
    std::process::exit(cli::main(std::env::args_os()));
}
