// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! No production signature names the series representation, except where naming the STORED or
//! REPORTED form is the point.
//!
//! # WHY THIS GUARD EXISTS, AND WHAT IT REPLACES
//!
//! Eight resident maps hold `object key -> (timestamp -> page address)`. The seam that let them be
//! narrowed one at a time required their helpers to take the OPERATIONS rather than the container.
//! That seam was built by following one function's neighbourhood and covered **four** signatures.
//! There were **nine**. The fifth was found when a migration failed to compile against it, and the
//! remaining four after that only because the type was finally used as the enumeration.
//!
//! So the claim "the seam is complete" was an assurance, twice, and wrong once. This makes it a
//! checked property.
//!
//! # THE OPERATIONAL LESSON, WHICH IS WORTH MORE THAN THE SEAM
//!
//! A seam is only as general as the dimension it varied -- and that includes WHICH CALLERS WERE
//! SAMPLED, not only which type parameter was abstracted. Proximity finds the first few and is
//! never a population.
//!
//! **When the population is characterised by a type, the type is the enumeration.** A one-line
//! query on the type was available the whole time and was worth more than the neighbourhood walk.
//! Any claim of the form "all the places that X" should name the mechanical query that produced
//! it, so a reader can re-run it rather than trust it. This test IS that query.
//!
//! # WHAT COUNTS AS PRODUCTION, STATED RATHER THAN ASSUMED
//!
//! Every `.rs` file under the crate's `src/`, excluding any whose path has a `tests` directory
//! component. The scope is derived from the crate root and walked recursively -- NOT from a list
//! of directories, because a hand-listed scope is the same sampling error inside the detector. A
//! signature in `storage_reporting.rs` or `packed_blocks.rs` is exactly what the original walk
//! missed.
//!
//! A `#[cfg(test)]` region inside a production FILE is not excluded. That is deliberate and it is
//! the cheaper rule: tracking attribute regions by brace counting is fragile, and the alternative
//! -- an allowlist entry with a reason -- is both checkable and self-documenting. One such entry
//! exists and says so.
//!
//! rust-internal: reads the crate's own source, no product behaviour

/// The representation the seam exists to stop naming.
const CONCRETE_SERIES: &str = "BTreeMap<u64, ElementEntry>";

/// Where naming it is CORRECT, each with the reason, keyed by file and enclosing function.
///
/// The distinction every entry shares: these name the STORED or REPORTED form rather than
/// operating on a live series. A stored shape genuinely is a plain map, and a report that projects
/// every kind into one uniform form is doing conversion, not leaking a representation.
const ALLOWED: &[(&str, &str, &str)] = &[
    (
        "storage_reporting.rs",
        "timestamped_kv_series",
        "A REPORTING PROJECTION. It collects every kind into one uniform shape for a report, and \
         returns `Cow`, so a kind on a different representation converts into it. Converting into \
         a reporting form is the point of the function; it does not operate on a live series.",
    ),
    (
        "grouped_series_index.rs",
        "from_plain",
        "A DESERIALIZER, naming the STORED form. The stored shape of this index genuinely is a \
         plain map; this is the conversion from it into the internal representation. Naming the \
         wire form is not leaking the in-memory one.",
    ),
    (
        "grouped_series_index.rs",
        "plain",
        "A TEST FIXTURE inside a production file. It builds the stored form to feed the \
         deserializer above. Allowlisted rather than excluded because this guard's definition of \
         production is a path rule, which is checkable, where excluding `#[cfg(test)]` regions \
         would need brace tracking, which is not.",
    ),
    // ---------------------------------------------------------------------------------------------
    // THE EVENT SERIES MOVING TO THE OWNED ARRAY. Eight entries arrive together because one map
    // changing representation is exactly what this seam was built to allow, and the boundary where
    // it meets its STORED shape is where the B-tree keeps being named. Every entry below is a
    // conversion at that boundary or a door onto the compatibility field that holds the stored
    // image -- none of them operates on a live series, which is the distinction every earlier
    // entry shares. TWO OF THEM ARE DETECTOR ARTEFACTS AND SAY SO, so that the narrowing filed as
    // #2172 can find and delete them instead of inheriting them as policy.
    // ---------------------------------------------------------------------------------------------
    (
        "engine_storage_state.rs",
        "set_context_event_series",
        "A DOOR ONTO THE STORED SHAPE. It takes the `BTreeMap` because its callers have one -- the\
         key-state fold deserializes the stored form, which is still a B-tree -- and converts into\
         the array here so the conversion lives in one place rather than at each caller. Same\
         category as `from_plain`: naming the wire form is not leaking the in-memory one.",
    ),
    (
        "object_record.rs",
        "from",
