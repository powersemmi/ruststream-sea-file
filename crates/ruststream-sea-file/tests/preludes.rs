//! Every prelude of this crate must leave the framework's own names alone.
//!
//! Each module below globs one prelude and then names the bare `Publish` as a trait bound. That
//! resolves only while `Publish` is the framework's slot capability trait: re-exporting a publish
//! policy under the bare name would win over the `ruststream::prelude::*` glob and turn these
//! into "expected trait, found struct". The policies therefore stay prefixed, which is what the
//! test below reads back.

mod crate_wide {
    use ruststream_sea_file::prelude::*;

    fn _publish_is_the_core_slot_trait<T: Publish>() {}

    pub(super) fn policies() -> (FilePublish, StdioPublish) {
        (FilePublish, StdioPublish)
    }
}

mod file_form {
    use ruststream_sea_file::file::prelude::*;

    fn _publish_is_the_core_slot_trait<T: Publish>() {}

    pub(super) fn policy() -> FilePublish {
        FilePublish
    }
}

mod stdio_form {
    use ruststream_sea_file::stdio::prelude::*;

    fn _publish_is_the_core_slot_trait<T: Publish>() {}

    pub(super) fn policy() -> StdioPublish {
        StdioPublish
    }
}

#[test]
fn every_prelude_carries_its_policies_under_their_prefixed_names() {
    let (file, stdio) = crate_wide::policies();
    assert_eq!(format!("{file:?}"), "FilePublish");
    assert_eq!(format!("{stdio:?}"), "StdioPublish");
    assert_eq!(format!("{:?}", file_form::policy()), "FilePublish");
    assert_eq!(format!("{:?}", stdio_form::policy()), "StdioPublish");
}
