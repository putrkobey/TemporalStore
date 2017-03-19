// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! A field the encoder stops writing must still decode an image that lacks it.
//!
//! # THE TRAP, AND WHY THE IMAGE CANNOT SHOW IT TO YOU
//!
//! `#[serde(skip_serializing)]` stops a key being written. `#[serde(default)]` makes a missing key
//! decode instead of failing. They are two halves of one decision, and the first without the
//! second is a store that cannot be opened: the first image written without the key makes every
//! subsequent load of it FAIL rather than decode an empty field.
//!
//! Measured rather than reasoned, because the two attributes differ in exactly the way that
//! matters and in no way that is visible:
//!
//! ```text
//! skip             + missing key -> OK   (Default is supplied automatically)
//! skip_serializing + missing key -> ERR  missing field
//! skip             writes: {"kept":0}
//! skip_serializing writes: {"kept":0}
//! ```
//!
//! BOTH WRITE THE SAME BYTES. The stored image gives no way to tell them apart; only the decode
//! side differs. That is why this needs a guard over the source rather than a check over an image.
//!
//! And it is why `skip` is NOT a trigger here: `skip_deserializing` supplies `Default` itself, so
//! `#[serde(skip)]` alone is correct, and flagging it would refuse a shape this tree uses on
//! purpose.
//!
//! # WHY NOW, AND WHICH FIELDS
//!
//! The consolidation is moving series maps off the shape they are stored in, and the way a map
//! stops being written is by gaining `skip_serializing`. So the trap is reachable, repeatedly.
//!
//! Four `ShardState` fields carry NO serde attribute at all. That reads as `#[serde(default)]`
//! while the key is still written -- both always write it -- and is not `default` on decode. Each
//! one therefore needs `default` added in the SAME change that stops writing it, never after.
//! The list is printed by this test rather than written here, so it cannot go stale.
//!
//! # HOW TWO AUDITS OF THIS GOT IT WRONG, WHICH IS WHY THE MATCHER IS SHAPED AS IT IS
//!
//! An attribute is bound to a field by CONTIGUITY, and a doc comment between them breaks the run.
//! An audit that paired attributes to fields by adjacency silently gave a bare field the PREVIOUS
//! field's attribute -- so a table whose shape cannot represent "absent" reported absent as
//! whatever came before it.
//!
//! A second attempt matched the attribute name as TEXT, which counted the dozen prose comments in
//! `engine_storage_state.rs` that explain why some field carries it -- reporting nine violations where there are
//! none.
//!
//! Both are guarded below by controls rather than by care.
//!
//! rust-internal: guards this crate's own stored shape, no product behaviour

/// One field of the struct: its line, its name, and the attributes CONTIGUOUSLY above it.
struct Field {
    line: usize,
    name: String,
    attributes: Vec<String>,
}

impl Field {
    /// Whether anything stops this field being written. `skip` is deliberately not included --
    /// see the module header for the measurement.
    fn stops_being_written(&self) -> bool {
        self.attributes
            .iter()
            .any(|attribute| attribute.contains("skip_serializing"))
    }

    /// Whether a missing key decodes rather than failing.
    fn decodes_a_missing_key(&self) -> bool {
        self.attributes
            .iter()
            .any(|attribute| attribute.contains("default"))
    }
}

/// Every field declared in the named struct's body, with the attributes contiguously above it.
///
/// BOUNDED TO THE STRUCT, not to the file, so this is a property of `ShardState` rather than of
/// whatever else `engine_storage_state.rs` happens to contain.
///
/// CONTIGUOUS, which is the part the adjacency audit got wrong: the walk upward stops at the first
/// line that is not an attribute, so a doc comment, a plain comment or a blank line ends the run
/// and a field with nothing of its own reports NOTHING rather than its neighbour's.
fn fields_of(source: &str, declaration: &str) -> Vec<Field> {
    let lines: Vec<&str> = source.lines().collect();
    let start = lines
        .iter()
        .position(|line| line.starts_with(declaration))
        .unwrap_or_else(|| panic!("{declaration} is in the source"));
    let end = (start + 1..lines.len())
        .find(|&index| lines[index] == "}")
        .expect("the struct body is closed at column zero");

    let mut fields = Vec::new();
    for index in start + 1..end {
        let trimmed = lines[index].trim();
        // A field declaration, not an attribute, comment or nested item.
        let Some((name, rest)) = trimmed.split_once(':') else {
            continue;
        };
        // NOT A PATH SEGMENT. A multi-line field declaration continues with lines like
        // `super::storage_bucket_internals::StoredModelKind,`, whose first colon splits off
        // `super` -- a lowercase word that reads exactly like a field name. One of those was
        // reported as a bare field, and it was visible only because this test PRINTS what it
        // finds rather than only asserting over it. A real field's name is followed by one colon.
        if rest.starts_with(':') {
            continue;
        }
        let name = name.trim_start_matches("pub(super) ").trim_start_matches("pub ").trim();
        if name.is_empty() || !name.chars().all(|c| c.is_ascii_lowercase() || c == '_') {
            continue;
        }
        let mut attributes = Vec::new();
        let mut above = index;
        while above > start + 1 {
            above -= 1;
            let candidate = lines[above].trim();
            if candidate.starts_with("#[") {
                attributes.insert(0, candidate.to_string());
            } else if candidate.starts_with("//") || candidate.is_empty() {
                // A comment is not an item and does not end an attribute's reach. The first
                // version of this walk stopped here, which reported a field as bare when its
                // attribute sat three doc-comment lines above it.
                continue;
            } else {
                // Anything else is the previous item, which does end it.
                break;
            }
        }
        fields.push(Field { line: index + 1, name: name.to_string(), attributes });
    }
    fields
}

/// A FIELD THAT STOPS BEING WRITTEN MUST STILL DECODE AN IMAGE THAT LACKS IT.
#[test]
fn a_field_that_stops_being_written_must_still_decode_an_image_that_lacks_it() {
    let source = include_str!("../engine_storage_state.rs");
    assert!(
        source.len() > 100_000,
        "the source this guard reads came back implausibly short ({} bytes), so a clean result \
         means nothing was scanned",
        source.len()
    );

    let fields = fields_of(source, "pub(super) struct ShardState {");
    // A NON-VACUITY FLOOR, NOT A BUDGET, AND IT HAD TO COME DOWN. This refuses to run
    // unless the matcher finds a plausible number of fields, so a matcher that silently
    // stops matching cannot satisfy the property below over an empty set. It was 40.
    // `ShardState` has 33 fields now -- the map consolidation removed thirteen -- so the
    // floor was failing for the OPPOSITE of the reason it was written: the structure
    // shrank, not the matcher. Thirty still catches a matcher that finds nothing or a
    // handful, and is NOT a target: a floor on a shrinking structure has to track the
    // structure or it becomes a tripwire for progress.
    assert!(
        fields.len() >= 30,
        "only {} field(s) were found in the ShardState body, which has around 33 -- the
         field matcher is not matching, so every assertion below is over an almost empty
         set",
        fields.len()
    );

    // --- THE PROPERTY ---
    let unpaired: Vec<&Field> = fields
        .iter()
        .filter(|field| field.stops_being_written() && !field.decodes_a_missing_key())
        .collect();
    assert!(
        unpaired.is_empty(),
        "these fields stop being written WITHOUT making a missing key decode:\n{}\n\
         \n\
         Each one is a store that cannot be opened. `skip_serializing` means the key is not \
         written into a new image; without `default`, the first image written without it makes \
         every subsequent load of that image FAIL rather than decode an empty field. Add \
         `default` in the same change, never as a follow-up.\n\
         \n\
         `#[serde(skip)]` is NOT this problem and is not reported here: `skip_deserializing` \
         supplies `Default` itself.",
        unpaired
            .iter()
            .map(|field| format!("  engine_storage_state.rs:{} {}", field.line, field.name))
            .collect::<Vec<_>>()
            .join("\n")
    );

    // --- NON-VACUOUS. Without this the test passes while watching nothing, and it would keep
    // --- passing through the whole migration, which is when it matters most.
    let stopping: Vec<&Field> = fields.iter().filter(|f| f.stops_being_written()).collect();
    // THE COUNT FELL, AND HERE IS WHICH ONE WENT. It was two. `context_events_compat` carried
