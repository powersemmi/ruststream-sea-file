//! [`FileSubscriber`]: a stream of deliveries backed by a driver task that also serves
//! repositioning.
//!
//! The client's `seek`/`rewind` need `&mut Consumer` and are explicitly not cancel-safe, so
//! a driver task owns the consumer: seeks arrive as commands and run to completion outside
//! any `select!`, while `next()` (which is cancel-safe) feeds the delivery channel.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use futures::Stream;
use ruststream::Subscriber;
use sea_streamer_file::{FileConsumer, FileErr};
use sea_streamer_types::{Consumer as _, SeqPos, StreamErr, Timestamp};
use tokio::sync::{mpsc, oneshot};

use crate::error::{SeaFileError, box_err};
use crate::message::{FilePosition, SeaMessage};

/// How many undelivered messages may sit between the driver and the consumer.
const CHANNEL_CAPACITY: usize = 64;

pub(crate) struct SeekCmd {
    position: FilePosition,
    done: oneshot::Sender<Result<(), SeaFileError>>,
}

pub(crate) struct Stamped {
    epoch: u64,
    item: Option<Result<SeaMessage, SeaFileError>>,
}

/// A subscription to one stream key in the file; yields [`SeaMessage`]s.
///
/// Dropping the subscriber stops the driver task. A replay subscription completes (the
/// stream ends) at the end of the file.
pub struct FileSubscriber {
    stream: String,
    rx: mpsc::Receiver<Stamped>,
    cmd: mpsc::UnboundedSender<SeekCmd>,
    epoch: Arc<AtomicU64>,
}

impl std::fmt::Debug for FileSubscriber {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FileSubscriber")
            .field("stream", &self.stream)
            .finish_non_exhaustive()
    }
}

impl FileSubscriber {
    /// The stream key this subscription consumes.
    #[must_use]
    pub fn stream_key(&self) -> &str {
        &self.stream
    }

    pub(crate) fn spawn(stream: String, consumer: FileConsumer, replay: bool) -> Self {
        let (out_tx, out_rx) = mpsc::channel(CHANNEL_CAPACITY);
        let (cmd_tx, cmd_rx) = mpsc::unbounded_channel();
        let epoch = Arc::new(AtomicU64::new(0));
        tokio::spawn(drive(
            consumer,
            out_tx,
            cmd_rx,
            stream.clone(),
            replay,
            Arc::clone(&epoch),
        ));
        Self {
            stream,
            rx: out_rx,
            cmd: cmd_tx,
            epoch,
        }
    }
}

impl Subscriber for FileSubscriber {
    type Message = SeaMessage;
    type Error = SeaFileError;

    fn stream(&mut self) -> impl Stream<Item = Result<SeaMessage, SeaFileError>> + Send + '_ {
        // Poll the channel in place rather than wrapping it in an owning stream, so `stream`
        // can be called again after the returned stream is dropped (the runtime and the
        // conformance helpers re-enter it per call). Items queued under an older generation
        // (before a seek) are discarded here; `item: None` marks a clean end of a replay.
        futures::stream::poll_fn(move |cx| {
            loop {
                match self.rx.poll_recv(cx) {
                    std::task::Poll::Ready(Some(stamped)) => {
                        if stamped.epoch == self.epoch.load(Ordering::Acquire) {
                            return std::task::Poll::Ready(stamped.item);
                        }
                    }
                    std::task::Poll::Ready(None) => return std::task::Poll::Ready(None),
                    std::task::Poll::Pending => return std::task::Poll::Pending,
                }
            }
        })
    }
}

/// Repositions a [`FileSubscriber`] while its stream runs; minted by
/// [`Seekable::seeker`](ruststream::Seekable::seeker).
#[derive(Clone)]
pub struct FileSeeker {
    cmd: mpsc::UnboundedSender<SeekCmd>,
    epoch: Arc<AtomicU64>,
    stream: String,
}

impl std::fmt::Debug for FileSeeker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FileSeeker")
            .field("stream", &self.stream)
            .finish_non_exhaustive()
    }
}

impl ruststream::Seeker for FileSeeker {
    type Position = FilePosition;
    type Error = SeaFileError;

    async fn seek(&self, to: FilePosition) -> Result<(), SeaFileError> {
        // Bump the generation first: deliveries already queued (or an in-flight forward)
        // belong to the pre-seek position and are discarded on the way out.
        self.epoch.fetch_add(1, Ordering::Release);
        let (done, wait) = oneshot::channel();
        self.cmd
            .send(SeekCmd { position: to, done })
            .map_err(|_| SeaFileError::Seek {
                stream: self.stream.clone(),
                source: Box::from("the subscription's driver task has shut down"),
            })?;
        wait.await.map_err(|_| SeaFileError::Seek {
            stream: self.stream.clone(),
            source: Box::from("the subscription's driver task has shut down"),
        })?
    }
}

impl ruststream::Seekable for FileSubscriber {
    type Seeker = FileSeeker;

    fn seeker(&self) -> FileSeeker {
        FileSeeker {
            cmd: self.cmd.clone(),
            epoch: Arc::clone(&self.epoch),
            stream: self.stream.clone(),
        }
    }
}

/// A receive failure that means the stream ended cleanly: the writer's end-of-stream mark,
/// or the end of a dead file in replay mode.
fn is_clean_end(err: &StreamErr<FileErr>) -> bool {
    matches!(
        err,
        StreamErr::Backend(FileErr::StreamEnded | FileErr::NotEnoughBytes)
    )
}

async fn drive(
    mut consumer: FileConsumer,
    out: mpsc::Sender<Stamped>,
    mut cmd_rx: mpsc::UnboundedReceiver<SeekCmd>,
    stream: String,
    replay: bool,
    epoch: Arc<AtomicU64>,
) {
    loop {
        // Captured before awaiting: a delivery resolved out of `next()` was positioned before
        // any seek that lands mid-await, so it must carry the pre-await generation - stamping
        // after the await would let a concurrent seek's bump leak onto a stale delivery.
        let current = epoch.load(Ordering::Acquire);
        tokio::select! {
            biased;
            cmd = cmd_rx.recv() => {
                let Some(SeekCmd { position, done }) = cmd else { break };
                // The client's seek is not cancel-safe: it runs here to completion, never
                // inside a racing select arm.
                let result = match position {
                    FilePosition::Beginning => consumer.rewind(SeqPos::Beginning).await,
                    FilePosition::End => consumer.rewind(SeqPos::End).await,
                    FilePosition::Sequence(sequence) => {
                        consumer.rewind(SeqPos::At(sequence)).await
                    }
                    FilePosition::Timestamp(millis) => {
                        let nanos = i128::from(millis) * 1_000_000;
                        match Timestamp::from_unix_timestamp_nanos(nanos) {
                            Ok(timestamp) => consumer.seek(timestamp).await,
                            Err(err) => {
                                let _ = done.send(Err(SeaFileError::Invalid(format!(
                                    "'{millis}' is not a valid timestamp: {err}"
                                ))));
                                continue;
                            }
                        }
                    }
                };
                let _ = done.send(result.map_err(|e| SeaFileError::Seek {
                    stream: stream.clone(),
                    source: box_err(e),
                }));
            }
            () = out.closed() => break,
            next = consumer.next() => {
                match next {
                    Ok(message) => {
                        let item = Stamped {
                            epoch: current,
                            item: Some(Ok(SeaMessage::new(&message))),
                        };
                        if out.send(item).await.is_err() {
                            break;
                        }
                    }
                    Err(err) if is_clean_end(&err) => {
                        if replay {
                            // A finished replay completes the subscription.
                            let _ = out.send(Stamped { epoch: current, item: None }).await;
                        } else {
                            let _ = out
                                .send(Stamped {
                                    epoch: current,
                                    item: Some(Err(SeaFileError::Receive {
                                        stream: stream.clone(),
                                        source: box_err(err),
                                    })),
                                })
                                .await;
                        }
                        break;
                    }
                    Err(err) => {
                        let _ = out
                            .send(Stamped {
                                epoch: current,
                                item: Some(Err(SeaFileError::Receive {
                                    stream: stream.clone(),
                                    source: box_err(err),
                                })),
                            })
                            .await;
                        break;
                    }
                }
            }
        }
    }
}
