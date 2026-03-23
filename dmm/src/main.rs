mod hashlist;

use hashlist::{decode_hashlist, parse_hashlist_html, HashlistEntry};

/// Example HTML hashlist files (as they appear in the GitHub repo).
/// In production these would be fetched from the hashlists repo.
const EXAMPLE_HASHLISTS: &[(&str, &str)] = &[
    (
        "single-entry.html",
        r#"<!doctype html><html><head><meta charset=UTF-8><title>Debrid Media Manager Hash List</title><style>iframe{border:none;position:absolute;top:0;left:0;width:100%;height:100%}</style></head><body><iframe src="https://debridmediamanager.com/hashlist#N4IgNglgzgLiBcBtUAzCYCmA7AhgWwwRACEIBzAOmIFcBjAayuqywE8KAmABi4A4KAjHy4AHKmGoAlHOwAeHAGwAWEABoQACxxQNRACZ7eHAKzGMtPbQDstHKZQAjLg4cpeAgMwcPhq1wUceg4CKAp6ArRqIFAQAF6E8AJKCrwKSQrGCgC+ALpZQA"></iframe></body></html>"#,
    ),
    (
        "multi-entry.html",
        r#"<!doctype html><html><head><meta charset=UTF-8><title>Debrid Media Manager Hash List</title><style>iframe{border:none;position:absolute;top:0;left:0;width:100%;height:100%}</style></head><body><iframe src="https://debridmediamanager.com/hashlist#N4IgLglmA2CmIC4QBVYGcwAIDCB7acAxpLgHYgA0I0EGiA2qAGYRykCGAtvEgMoSkwsaADoATAAYAjBJEAWANIiAqgAkAIiIBC0AK4AldgE8RADzEA2AKwBaVQFEAatkogAFuzRvEIC+wCcAOxW-gBGTEwAJlaEEuxM1gDMUv5B-kyhgQAciWJSWelyTImEiVaRrmgQAF48cmL+cv4WgQ0WAL4UzKywHNw+qOwATmgiuEwivELC4tJiIjJZEgAOIgDq9lo26gAyIgCC+9jzsqqWcq4eXj7sWVlR7CFWcrAFUtmlrYFSfn7ZoVl8hJYJE5JF2JFEpUajxAok5BIJLlEZ1umwuDwQPY4MsPIJRuohrAuLMJBYRK0Vto9IYTOpkLwzOdLp5vEhQokmFJYIlQYQrOwLJkCnN4VYWgV2KFCJFYExRXJxYFobVEHk5IE5DkLFr2gBddpAA"></iframe></body></html>"#,
    ),
    (
        "dmm-format.html",
        r#"<!doctype html><html><head><meta charset=UTF-8><title>Debrid Media Manager Hash List</title><style>iframe{border:none;position:absolute;top:0;left:0;width:100%;height:100%}</style></head><body><iframe src="https://debridmediamanager.com/hashlist#N4IgLglmA2CmIC4QBECyqAEAxA9gJwFsBDMEAGnHz1gDswBnRAbVADMI4aiD4lUcAbhFgA6AEwAGMQBYRARgkAOCQAcRBANYDyIABZF6uxCCJEARmYDGlgCY25csWIDMz6dICsHgGzeA7H6KigCcwRISphbWdg5OOmYAnmCwjAiS4RkZAL4AullAA"></iframe></body></html>"#,
    ),
];

fn format_size(bytes: u64) -> String {
    const GB: f64 = 1_073_741_824.0;
    const MB: f64 = 1_048_576.0;
    let b = bytes as f64;
    if b >= GB {
        format!("{:.2} GB", b / GB)
    } else {
        format!("{:.2} MB", b / MB)
    }
}

fn print_record(idx: usize, entry: &HashlistEntry) {
    println!("  Record #{idx}:");
    println!("    filename : {}", entry.filename);
    println!("    hash     : {}", entry.hash);
    println!("    magnet   : magnet:?xt=urn:btih:{}", entry.hash);
    println!("    size     : {} ({} bytes)", format_size(entry.size), entry.size);
    println!();
}

fn main() {
    println!("=== DMM Hashlist Ingestor ===");
    println!();

    let mut total_records = 0;

    for (name, html) in EXAMPLE_HASHLISTS {
        println!("--- Processing hashlist: {name} ---");

        match parse_hashlist_html(html) {
            Ok(hashlist) => {
                if let Some(title) = &hashlist.title {
                    println!("  Title: {title}");
                }
                println!("  Entries: {}", hashlist.list.len());
                println!();

                for (i, entry) in hashlist.list.iter().enumerate() {
                    print_record(i + 1, entry);
                    total_records += 1;
                }

                println!("  [OK] Hashlist {name} fully processed ({} records)", hashlist.list.len());
            }
            Err(e) => {
                println!("  [ERROR] Failed to parse {name}: {e}");
            }
        }
        println!();
    }

    // Also demo direct encoded string decoding (bare array format)
    println!("--- Processing bare-array encoded hashlist ---");
    let bare = "NobwRAZglgNgpgOwIYFs5gFxgMoAskBOcAJgHQCyA9gG5RykBMADAwCykoDW1YANGPgDOuTGACMDAMysArADYA7AA4AnEyQAjAMbE4ECdPnK1mnXoOzFSvmA0BPAC5xBmGU3cf3AXwC6QA";
    match decode_hashlist(bare) {
        Ok(hashlist) => {
            println!("  Entries: {}", hashlist.list.len());
            println!();
            for (i, entry) in hashlist.list.iter().enumerate() {
                print_record(i + 1, entry);
                total_records += 1;
            }
            println!("  [OK] Bare-array hashlist fully processed");
        }
        Err(e) => println!("  [ERROR] {e}"),
    }

    println!();
    println!("=== Summary: {total_records} total records processed ===");
}
