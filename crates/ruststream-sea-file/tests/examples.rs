//! The tests of the examples, one module per example, each gated on the example's
//! `required-features`. The manifest declares the examples, and says why each one listed here
//! keeps a test binary of its own.

ruststream_example_suite::examples!(
    file_start_at,
    manual_file_start_at,
    file_testing_live,
    manual_file_testing_live,
    guide_event_log,
    manual_guide_event_log,
    guide_resume,
    manual_guide_resume,
    guide_retries,
    manual_guide_retries,
    guide_testing,
    manual_guide_testing,
    sea_acknowledgement,
    manual_sea_acknowledgement,
    sea_headers,
    manual_sea_headers,
);
