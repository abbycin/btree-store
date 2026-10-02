use crate::cli::{Class, CliError, EXIT_FAILURE, EXIT_OK_SUMMARY};
use btree_store::{
    BucketStats, CheckError, CheckOptions, CheckReportWithSpace, CheckStatus, DiagnosticSeverity,
    FORMAT_VERSION, check_path_with_options,
};
use std::fmt::Write;
use std::path::Path;

/// What the caller asked `check` to print. Every spelling runs the same check;
/// the flags only choose which keys reach stdout.
pub(crate) struct Options {
    pub(crate) summary: bool,
    pub(crate) scan: bool,
}

/// Maps a `check_path` failure to its class. `check` and `compact` share this so a
/// damaged file is classified the same way by both commands.
pub(crate) fn error_class(error: &CheckError) -> Class {
    match error {
        CheckError::Io(_)
        | CheckError::NotRegularFile { .. }
        | CheckError::ChangedDuringCheck { .. } => Class::Io,
        CheckError::LockBusy { .. } => Class::LockBusy,
        CheckError::NoValidMeta => Class::SourceCorrupt,
        CheckError::UnsupportedFormatVersion { .. } => Class::SourceVersionUnsupported,
        CheckError::MixedFormatVersions { .. } => Class::SourceVersionMixed,
    }
}

/// Maps a report that opened but did not pass to its class.
pub(crate) fn status_class(status: CheckStatus) -> Class {
    if status == CheckStatus::Failed {
        Class::CheckFailed
    } else {
        Class::CheckIncomplete
    }
}

pub(crate) fn run(path: &Path, options: Options) -> i32 {
    // The space fields come out of the walk either way; only `--scan` asks for
    // the per-bucket counters, and a summary run must not pay for what it will
    // not print.
    match check_path_with_options(path, &CheckOptions { scan: options.scan }) {
        Ok(checked) => print_report(&checked, &options),
        Err(error) => {
            eprintln!(
                "{}",
                CliError::new("check", error_class(&error), error.to_string())
            );
            EXIT_FAILURE
        }
    }
}

/// Appends one bucket's seven lines to `out`.
///
/// A bucket name is a catalog key: any byte is legal in it, including the
/// separator this protocol is built from and the newline that ends a line.
/// Percent-encoding everything outside the unreserved set keeps one bucket
/// inside one seven-line block whatever its name holds.
fn write_bucket(out: &mut String, bucket: &BucketStats) {
    let _ = writeln!(
        out,
        "bucket.name={}",
        percent_encode(bucket.name.as_bytes())
    );
    let _ = writeln!(out, "bucket.prefix_encoding={}", bucket.prefix_encoding);
    let _ = writeln!(out, "bucket.records={}", bucket.records);
    let _ = writeln!(out, "bucket.logical_key_bytes={}", bucket.logical_key_bytes);
    let _ = writeln!(
        out,
        "bucket.logical_value_bytes={}",
        bucket.logical_value_bytes
    );
    let _ = writeln!(out, "bucket.tree_height={}", bucket.tree_height);
    let _ = writeln!(out, "bucket.reachable_pages={}", bucket.reachable_pages);
}

fn is_unreserved(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-' | b'~')
}

fn percent_encode(bytes: &[u8]) -> String {
    // Every byte can cost three characters, so the exact bound is reserved up
    // front rather than regrown once per escape.
    let mut out = String::with_capacity(bytes.len() * 3);
    for byte in bytes {
        if is_unreserved(*byte) {
            out.push(*byte as char);
        } else {
            let _ = write!(out, "%{byte:02X}");
        }
    }
    out
}

fn print_report(checked: &CheckReportWithSpace, options: &Options) -> i32 {
    let report = &checked.report;
    match report.status {
        CheckStatus::Ok => {
            let space = checked
                .space
                .expect("a passing check publishes its space statistics");
            let data_pages = report
                .next_page_id
                .expect("a passing check selected a generation")
                - 2;
            let active_data_pages = report.reachable_pages + report.allocator_list_pages;
            // The report is built in memory and written once: a scan of a large
            // catalog would otherwise take the stdout lock and issue a write for
            // every key. `file=` is written as `Path::display()` renders it and
            // is the one value here a reader parses as a path rather than as a
            // field this protocol encodes.
            let mut out = String::new();
            if options.summary {
                let _ = writeln!(out, "status=ok");
                let _ = writeln!(out, "file={}", report.path.display());
                let _ = writeln!(out, "source_version={FORMAT_VERSION}");
                let _ = writeln!(out, "generation={}", report.generation.unwrap_or(0));
                let _ = writeln!(out, "file_len={}", report.file_len);
                let _ = writeln!(out, "next_page_id={}", report.next_page_id.unwrap_or(0));
                let _ = writeln!(out, "data_pages={data_pages}");
                let _ = writeln!(out, "reusable_pages={}", report.reusable_pages);
                let _ = writeln!(out, "reusable_bytes={}", space.reusable_bytes);
                let _ = writeln!(out, "reusable_extents={}", space.reusable_extent_count);
                let _ = writeln!(out, "retired_pages={}", report.retired_pages);
                let _ = writeln!(out, "retired_bytes={}", space.retired_bytes);
                let _ = writeln!(out, "retired_extents={}", space.retired_extent_count);
                let _ = writeln!(out, "allocator_list_pages={}", report.allocator_list_pages);
                let _ = writeln!(
                    out,
                    "reusable_if_all_retired_promoted_bytes={}",
                    space.reusable_if_all_retired_promoted_bytes
                );
                let _ = writeln!(out, "reachable_pages={}", report.reachable_pages);
                let _ = writeln!(out, "active_data_pages={active_data_pages}");
                let _ = writeln!(out, "trailing_bytes={}", report.trailing_bytes);
                let _ = writeln!(out, "cost=full-check");
            } else {
                let _ = writeln!(out, "status=ok");
                let _ = writeln!(out, "file={}", report.path.display());
                let _ = writeln!(out, "file_len={}", report.file_len);
                let _ = writeln!(out, "generation={}", report.generation.unwrap_or(0));
                let _ = writeln!(out, "next_page_id={}", report.next_page_id.unwrap_or(0));
                let _ = writeln!(out, "reachable_pages={}", report.reachable_pages);
                let _ = writeln!(out, "reusable_pages={}", report.reusable_pages);
                let _ = writeln!(out, "retired_pages={}", report.retired_pages);
                let _ = writeln!(out, "allocator_list_pages={}", report.allocator_list_pages);
                let _ = writeln!(out, "trailing_bytes={}", report.trailing_bytes);
            }
            if options.scan {
                if !options.summary {
                    let _ = writeln!(out, "source_version={FORMAT_VERSION}");
                    let _ = writeln!(out, "data_pages={data_pages}");
                    let _ = writeln!(out, "active_data_pages={active_data_pages}");
                    let _ = writeln!(out, "cost=full-check");
                }
                let _ = writeln!(out, "bucket_scan=true");
                for bucket in checked
                    .buckets
                    .as_deref()
                    .expect("a passing scan publishes its buckets")
                {
                    out.push('\n');
                    write_bucket(&mut out, bucket);
                }
            }
            for diagnostic in &report.diagnostics {
                let _ = writeln!(out, "diagnostic: {}", format_diagnostic(diagnostic));
            }
            print!("{out}");
            EXIT_OK_SUMMARY
        }
        CheckStatus::Failed | CheckStatus::Incomplete => {
            let class = status_class(report.status);
            eprintln!(
                "{}",
                CliError::new(
                    "check",
                    class,
                    format!("{} diagnostic(s)", report.diagnostics.len())
                )
            );
            for diagnostic in &report.diagnostics {
                eprintln!("diagnostic: {}", format_diagnostic(diagnostic));
            }
            EXIT_FAILURE
        }
    }
}

fn format_diagnostic(diagnostic: &btree_store::CheckDiagnostic) -> String {
    let severity = match diagnostic.severity {
        DiagnosticSeverity::Error => "error",
        DiagnosticSeverity::Warning => "warning",
    };
    // Only the two fields that carry file bytes are escaped: `actual` holds raw key
    // bytes, and a bucket tree's `reference_path` embeds a catalog key. Everything
    // else is crate-controlled — `code` and `check` are rule text, `generation`
    // and `pid` are integers — so it stays readable: `check` is a sentence and
    // contains spaces, which is already the field separator, so the line is read
    // by `key=` prefix and not by splitting on whitespace.
    format!(
        "severity={severity} code={} check={} generation={} pid={} path={} expected={} actual={}",
        diagnostic.code,
        diagnostic.check,
        diagnostic
            .generation
            .map(|value| value.to_string())
            .unwrap_or_else(|| "-".to_string()),
        diagnostic
            .pid
            .map(|value| value.to_string())
            .unwrap_or_else(|| "-".to_string()),
        percent_encode(
            diagnostic
                .reference_path
                .as_deref()
                .unwrap_or("-")
                .as_bytes()
        ),
        diagnostic.expected.as_deref().unwrap_or("-"),
        percent_encode(diagnostic.actual.as_deref().unwrap_or("-").as_bytes()),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The two report shapes must stay apart: a file that opened but did not pass
    /// is `failed`/`incomplete`, never `source-corrupt`. `incomplete` has no static
    /// fixture — `IO_READ` only happens when a referenced page cannot be read — so
    /// its mapping is pinned here instead.
    #[test]
    fn a_report_that_opened_never_maps_to_source_corrupt() {
        assert_eq!(status_class(CheckStatus::Failed), Class::CheckFailed);
        assert_eq!(
            status_class(CheckStatus::Incomplete),
            Class::CheckIncomplete
        );
        assert_eq!(status_class(CheckStatus::Failed).as_str(), "failed");
        assert_eq!(status_class(CheckStatus::Incomplete).as_str(), "incomplete");
    }

    /// `check_path` has two failure shapes and both are classified per variant.
    #[test]
    fn every_check_error_variant_has_a_class() {
        use std::path::PathBuf;
        let path = PathBuf::from("x");
        // `CheckError` is not `PartialEq`, so each variant is named rather than
        // collected; the point is that none of them falls through unmatched.
        assert_eq!(
            error_class(&CheckError::NotRegularFile { path: path.clone() }),
            Class::Io
        );
        assert_eq!(
            error_class(&CheckError::LockBusy { path: path.clone() }),
            Class::LockBusy
        );
        assert_eq!(error_class(&CheckError::NoValidMeta), Class::SourceCorrupt);
        assert_eq!(
            error_class(&CheckError::UnsupportedFormatVersion { found: 1 }),
            Class::SourceVersionUnsupported
        );
        assert_eq!(
            error_class(&CheckError::MixedFormatVersions { found: 1 }),
            Class::SourceVersionMixed
        );
        assert_eq!(
            error_class(&CheckError::ChangedDuringCheck {
                path: path.clone(),
                initial_len: 1,
                final_len: 0,
            }),
            Class::Io
        );
    }

    fn diagnostic(
        severity: DiagnosticSeverity,
        generation: Option<u64>,
        pid: Option<btree_store::PageId>,
        path: Option<&str>,
        expected: Option<&str>,
        actual: Option<&str>,
    ) -> btree_store::CheckDiagnostic {
        btree_store::CheckDiagnostic {
            severity,
            code: "SLOT_KEY_ORDER",
            check: "leaf keys ascend",
            generation,
            pid,
            reference_path: path.map(str::to_owned).map(String::into_boxed_str),
            expected: expected.map(str::to_owned).map(String::into_boxed_str),
            actual: actual.map(str::to_owned).map(String::into_boxed_str),
        }
    }

    /// The documented key order, with every field present. This is the wire
    /// format `docs/cli.md` shows, so it is pinned field for field: a renamed,
    /// reordered or re-punctuated line silently breaks every parser reading it.
    #[test]
    fn a_fully_populated_diagnostic_renders_the_documented_line() {
        let line = format_diagnostic(&diagnostic(
            DiagnosticSeverity::Error,
            Some(7),
            Some(42),
            Some("bucket/plain/node=3/child=0"),
            Some("0x0102"),
            Some("0x0304"),
        ));
        assert_eq!(
            line,
            "severity=error code=SLOT_KEY_ORDER check=leaf keys ascend \
             generation=7 pid=42 path=bucket%2Fplain%2Fnode%3D3%2Fchild%3D0 \
             expected=0x0102 actual=0x0304"
        );
    }

    /// An absent field is the documented `-`, and a warning renders as
    /// `warning`. The placeholders must survive escaping untouched, or a
    /// reader can no longer tell "no value" from a literal `-`.
    #[test]
    fn an_absent_field_renders_the_documented_placeholder() {
        let line = format_diagnostic(&diagnostic(
            DiagnosticSeverity::Warning,
            None,
            None,
            None,
            None,
            None,
        ));
        assert_eq!(
            line,
            "severity=warning code=SLOT_KEY_ORDER check=leaf keys ascend \
             generation=- pid=- path=- expected=- actual=-"
        );
    }

    /// The hazard this file already defends against for bucket names: key bytes
    /// are arbitrary, so a newline in `actual` used to append a forged stderr
    /// line and an `=` used to forge a key inside one. One diagnostic must stay
    /// one line carrying exactly the documented keys.
    ///
    /// File bytes reach only `path` (a bucket tree's path embeds a catalog key)
    /// and `actual` (the offending key bytes), so those are the two fields put
    /// under attack here. `expected` is always a crate literal and is left
    /// readable; `check` is the crate's rule text and carries spaces, so a line is
    /// read by its `key=` prefixes and never by splitting on whitespace.
    #[test]
    fn file_content_cannot_add_a_line_or_forge_a_key() {
        let line = format_diagnostic(&diagnostic(
            DiagnosticSeverity::Error,
            Some(1),
            Some(2),
            Some("bucket/a=b\ndiagnostic: severity=ok"),
            Some("strictly increasing"),
            Some("k\ninjected=1"),
        ));
        assert!(!line.contains('\n'), "{line}");
        assert_eq!(line.matches(" actual=").count(), 1, "{line}");
        assert_eq!(line.split(" actual=").nth(1).unwrap(), "k%0Ainjected%3D1");
        assert!(!line.contains("severity=ok"), "{line}");
        // `expected` stays readable: escaping a crate literal would only make the
        // rule unreadable for the operator reading the diagnostic.
        assert!(line.contains(" expected=strictly increasing "), "{line}");
    }
}
