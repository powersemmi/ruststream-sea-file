//! Each prelude of this crate must carry the vocabulary its side of a service writes.
//!
//! A mount site globs a transport prelude and names policies by concept, so the bare `Publish`
//! there is this form's policy: a value, constructible, handed to `after_startup` and to an
//! include site. A handler body globs `ruststream::prelude` instead and bounds an injected
//! publisher by the framework's capability traits. The two vocabularies live in different files,
//! which is what lets a form reuse the concept name.
//!
//! Each module below pins both halves for one prelude: the concept name resolves to this form's
//! policy value, and the framework's publisher capability is still nameable as a bound through
//! the same glob.

mod file_form {
    use ruststream_sea_file::file::prelude::*;

    fn _p<T: Publisher>() {}

    // The spelling a mount site writes. `Publish` naming a value rather than a trait is the
    // whole assertion; `Publish::default()` is the same thing and lives in the rustdoc example.
    pub(super) fn policy() -> Publish {
        Publish
    }
}

mod stdio_form {
    use ruststream_sea_file::stdio::prelude::*;

    fn _p<T: Publisher>() {}

    // The spelling a mount site writes. `Publish` naming a value rather than a trait is the
    // whole assertion; `Publish::default()` is the same thing and lives in the rustdoc example.
    pub(super) fn policy() -> Publish {
        Publish
    }
}

mod crate_wide {
    use ruststream_sea_file::prelude::*;

    fn _p<T: Publisher>() {}

    // Both forms would claim the bare name here, so a mount site spanning them writes the
    // prefixed ones.
    pub(super) fn policies() -> (FilePublish, StdioPublish) {
        (FilePublish, StdioPublish)
    }
}

#[test]
fn each_transport_prelude_names_its_own_policy_by_concept() {
    assert_eq!(format!("{:?}", file_form::policy()), "FilePublish");
    assert_eq!(format!("{:?}", stdio_form::policy()), "StdioPublish");

    let (file, stdio) = crate_wide::policies();
    assert_eq!(format!("{file:?}"), "FilePublish");
    assert_eq!(format!("{stdio:?}"), "StdioPublish");
}
