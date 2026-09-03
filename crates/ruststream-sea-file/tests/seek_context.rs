//! The keyed seek contract over real stream files: a running service reads the delivery's
//! position and the subscription's reposition handle off the file transport's contexts, a
//! rewind to a captured position redelivers exactly that message, and a page body reaches the
//! same handle through the subscription-scoped batch context.

mod common;

use std::io;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use ruststream_sea_file::FilePublisher;
use ruststream_sea_file::file::prelude::*;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;

const STEP_TIMEOUT: Duration = Duration::from_secs(20);

fn tmp_path(name: &str) -> String {
    static SEQ: AtomicU64 = AtomicU64::new(0);
    std::env::temp_dir()
        .join(format!(
            "ruststream-sea-ctx-{name}-{}-{}.ss",
            std::process::id(),
            SEQ.fetch_add(1, Ordering::Relaxed)
        ))
        .to_string_lossy()
        .into_owned()
}

#[derive(Debug, Outgoing, Serialize, Deserialize)]
struct Job {
    id: u64,
}

/// What the handlers report: the single-message form records the position it read by key, the
/// page form records the ids of one page. Each test builds its own instance with its own tick
/// channel, so waiting is on progress rather than on the clock.
#[derive(Clone)]
struct Recorder {
    deliveries: Arc<Mutex<Vec<(u64, FilePosition)>>>,
    pages: Arc<Mutex<Vec<Vec<u64>>>>,
    rewound: Arc<AtomicBool>,
    tick: mpsc::UnboundedSender<()>,
}

impl Recorder {
    fn new() -> (Self, mpsc::UnboundedReceiver<()>) {
        let (tick, ticks) = mpsc::unbounded_channel();
        let recorder = Self {
            deliveries: Arc::new(Mutex::new(Vec::new())),
            pages: Arc::new(Mutex::new(Vec::new())),
            rewound: Arc::new(AtomicBool::new(false)),
            tick,
        };
        (recorder, ticks)
    }

    fn record_delivery(&self, id: u64, at: FilePosition) {
        self.deliveries
            .lock()
            .expect("recorder mutex poisoned")
            .push((id, at));
        let _ = self.tick.send(());
    }

    fn record_page(&self, ids: Vec<u64>) {
        self.pages
            .lock()
            .expect("recorder mutex poisoned")
            .push(ids);
        let _ = self.tick.send(());
    }

    fn first_position(&self) -> FilePosition {
        self.deliveries.lock().expect("recorder mutex poisoned")[0].1
    }

    fn deliveries(&self) -> Vec<(u64, FilePosition)> {
        self.deliveries
            .lock()
            .expect("recorder mutex poisoned")
            .clone()
    }

    fn paged_ids(&self) -> Vec<u64> {
        self.pages.lock().expect("recorder mutex poisoned").concat()
    }
}

/// The application state, so the recorder reaches the handlers without a global.
#[derive(FromRef)]
struct AppState {
    recorder: Recorder,
}

/// Reads both per-delivery keys: the position is this delivery's, the seeker is the
/// subscription's. The second job rewinds to where the first one sat, exactly once - the pinned
/// contract says that redelivers the first job itself.
#[subscriber(FileStream::new("jobs"), start_at(FilePosition::beginning()))]
async fn rewind(
    job: &Job,
    State(recorder): State<Recorder>,
    Ctx(at): Ctx<Position>,
    Ctx(seeker): Ctx<SeekHandle>,
) -> HandlerOutcome {
    recorder.record_delivery(job.id, at);
    if job.id == 2
        && !recorder.rewound.swap(true, Ordering::SeqCst)
        && seeker.seek(recorder.first_position()).await.is_err()
    {
        return HandlerOutcome::retry();
    }
    HandlerOutcome::ack()
}

/// A page names the subscription-scoped context instead: the seek handle is shared by every
/// delivery of the page, while a per-delivery position has no meaning across one.
#[subscriber(FileStream::new("digest"), start_at(FilePosition::beginning()))]
async fn digest(
    page: &[Job],
    ctx: &mut Context<'_, FileBatchContext, AppState>,
) -> Vec<HandlerOutcome> {
    let live = ctx.context(SeekHandle).clone();
    let ids = page.iter().map(|job| job.id).collect::<Vec<_>>();
    ctx.state().recorder.record_page(ids);
    // The handle is live for the whole subscription, not just this page: repositioning to the
    // tail is a no-op here and proves the page reached a usable seeker.
    if live.seek(FilePosition::end()).await.is_err() {
        return page.iter().map(|_| HandlerOutcome::retry()).collect();
    }
    page.iter().map(|_| HandlerOutcome::ack()).collect()
}

/// Publishes `ids` to `stream` once the broker is connected and the subscriptions are open.
async fn publish(publisher: FilePublisher, stream: &'static str, ids: [u64; 2]) -> io::Result<()> {
    for id in ids {
        publisher
            .message(&Job { id })
            .to(stream)
            .publish()
            .await
            .map_err(io::Error::other)?;
    }
    Ok(())
}

/// Waits for `count` recorded steps, failing the test rather than hanging the suite.
async fn expect_steps(ticks: &mut mpsc::UnboundedReceiver<()>, count: usize) {
    for step in 0..count {
        tokio::time::timeout(STEP_TIMEOUT, ticks.recv())
            .await
            .unwrap_or_else(|_| panic!("step {step} must arrive"))
            .expect("the recorder outlives the handlers");
    }
}

#[test]
fn a_handler_rewinds_its_own_subscription_through_the_context_keys() {
    common::rt().block_on(async {
        let path = tmp_path("rewind");
        let (recorder, mut ticks) = Recorder::new();

        let state = recorder.clone();
        let app = RustStream::new(AppInfo::new("seek-context", "0.1.0"))
            .on_startup(async move |()| {
                Ok::<_, std::convert::Infallible>(AppState { recorder: state })
            })
            .with_broker(FileBroker::new(&path), |b| {
                b.after_startup(FilePublish, async move |publisher| {
                    publish(publisher, "jobs", [1, 2]).await
                });
                b.include(rewind);
            });

        let running = app.start().await.expect("app starts");
        // Two live deliveries, then the same two again after the handler's rewind.
        expect_steps(&mut ticks, 4).await;
        running.shutdown().await.expect("shutdown succeeds");

        let seen = recorder.deliveries();
        assert_eq!(
            seen.iter().map(|(id, _)| *id).collect::<Vec<_>>(),
            vec![1, 2, 1, 2],
            "the rewind must redeliver the captured message and everything after it"
        );
        assert_eq!(
            seen[0].1, seen[2].1,
            "a captured position is pinned: the redelivered message reports it again"
        );
        assert_eq!(seen[1].1, seen[3].1, "positions are stable across a rewind");

        let _ = std::fs::remove_file(&path);
    });
}

#[test]
fn a_page_body_reads_the_subscription_seeker_off_the_batch_context() {
    common::rt().block_on(async {
        let path = tmp_path("digest");
        let (recorder, mut ticks) = Recorder::new();

        let state = recorder.clone();
        let app = RustStream::new(AppInfo::new("batch-context", "0.1.0"))
            .on_startup(async move |()| {
                Ok::<_, std::convert::Infallible>(AppState { recorder: state })
            })
            .with_broker(FileBroker::new(&path), |b| {
                b.after_startup(FilePublish, async move |publisher| {
                    publish(publisher, "digest", [7, 8]).await
                });
                b.include(digest.buffered(nonzero!(2), Duration::from_millis(50)));
            });

        let running = app.start().await.expect("app starts");
        // One page of both, or two of one each, depending on how the buffer closes.
        expect_steps(&mut ticks, 1).await;
        while recorder.paged_ids().len() < 2 {
            expect_steps(&mut ticks, 1).await;
        }
        running.shutdown().await.expect("shutdown succeeds");

        assert_eq!(
            recorder.paged_ids(),
            vec![7, 8],
            "the page path must deliver every element, in order"
        );

        let _ = std::fs::remove_file(&path);
    });
}
