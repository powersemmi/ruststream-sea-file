//! The stage of `examples/stdio_payloads.rs` written without the macros: a body over borrowed
//! bytes answers with bytes, and the stdio publisher envelopes what a line would change.
//!
//! ```text
//! cargo test --example manual_stdio_payloads --features testing
//! cargo run --example manual_stdio_payloads --features testing -- run
//! ```

use std::future::{Future, ready};
use std::process::ExitCode;

use ruststream::runtime::cli::run_main;
use ruststream_sea_file::stdio::prelude::*;

// --8<-- [start:stage]
/// An image as it arrives, bytes the codec does not touch.
#[derive(Deserialized)]
struct Image<'a>(&'a [u8]);

/// The bytes this stage hands the next one.
#[derive(Outgoing, Serialized)]
#[outgoing(name = "thumbnails")]
struct Thumbnail(Vec<u8>);

/// Keeps the first kilobyte of each image as its thumbnail.
struct Shrink;

impl<'p> Handle<Image<'p>, Thumbnail> for Shrink {
    fn handle(
        &self,
        image: &Image<'p>,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<Thumbnail, HandlerOutcome>> {
        let kept = image.0.len().min(1024);
        tracing::info!(bytes = image.0.len(), kept, "thumbnail");
        ready(Ok(Thumbnail(image.0[..kept].to_vec())))
    }
}

fn app() -> impl App<State = ()> {
    RustStream::new(AppInfo::new("thumbnails", "0.1.0")).with_broker(StdioBroker::new(), |b| {
        b.include(subscriber("images", Shrink).reply().build())
            .out_retry(Publish)
            .to("images.retry");
    })
}
// --8<-- [end:stage]

fn main() -> ExitCode {
    run_main(app)
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_sea_file::stdio::prelude::*;

    use super::app;

    /// Bytes as an upstream stage writes them.
    #[derive(Outgoing, Serialized)]
    struct Upload(Vec<u8>);

    async fn passes_whole(bytes: &[u8]) -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;

        tb.broker::<StdioBroker>()
            .message(&Upload(bytes.to_vec()))
            .to("images")
            .publish()
            .await?;

        tb.broker::<StdioBroker>()
            .published::<()>("thumbnails")
            .assert_called_once()
            .with_raw(bytes);
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test]
    async fn a_binary_payload_crosses_the_pipe_whole() -> Result<(), Box<dyn Error>> {
        passes_whole(&[0x89, b'P', b'N', b'G', 0x00, 0xff, b'\n', 0x1a]).await
    }

    #[tokio::test]
    async fn a_multi_line_padded_payload_crosses_the_pipe_whole() -> Result<(), Box<dyn Error>> {
        passes_whole(b"  first line\nsecond line  ").await
    }
}
