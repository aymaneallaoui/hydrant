pub mod cli;
pub mod payload;
pub mod publisher;
pub mod stats;
pub mod subscriber;

use std::{
    pin::pin,
    sync::{Arc, atomic::Ordering::Relaxed},
    time::Duration,
};

use anyhow::{Context, bail};
use tokio::{
    sync::{Semaphore, mpsc, watch},
    time::{Instant, sleep, timeout},
};

use crate::{
    cli::Args,
    publisher::Publisher,
    stats::{Report, RunFacts, Shared, fd_hint},
    subscriber::{Ctx, Job},
};

const DRAIN_POLL: Duration = Duration::from_millis(5);
const TEARDOWN_TIMEOUT: Duration = Duration::from_secs(10);

pub async fn run(args: Args) -> anyhow::Result<Report> {
    let target = cli::resolve_target(&args.url).await?;
    let connections = args.connections()?;
    let shared = Arc::new(Shared::new());
    let mut ctrl_c = pin!(tokio::signal::ctrl_c());
    let mut interrupted = false;

    let (stop_tx, stop_rx) = watch::channel(false);
    let (alive_tx, mut alive_rx) = mpsc::channel::<()>(1);
    let (connected_tx, mut connected_rx) = mpsc::unbounded_channel();
    let ctx = Arc::new(Ctx {
        shared: Arc::clone(&shared),
        target: target.clone(),
        topic: args.topic.clone(),
        transport: args.transport,
        connected: connected_tx,
    });
    let permits = Arc::new(Semaphore::new(args.connect_concurrency()?));

    let mut publisher = Publisher::new(target, &args.topic);
    publisher
        .connect()
        .await
        .context("connecting the publisher connection")?;

    let connect_started = Instant::now();
    let connect_phase = async {
        for index in 0..connections {
            let permit = Arc::clone(&permits)
                .acquire_owned()
                .await
                .expect("semaphore is never closed");
            tokio::spawn(subscriber::run(Job {
                ctx: Arc::clone(&ctx),
                index,
                permit,
                stop: stop_rx.clone(),
                alive: alive_tx.clone(),
            }));
        }
        drop(alive_tx);
        for _ in 0..connections {
            connected_rx.recv().await;
        }
    };
    tokio::select! {
        () = connect_phase => {}
        _ = &mut ctrl_c => interrupted = true,
    }
    let connect_elapsed = connect_started.elapsed();

    let mut publish_start_nanos = None;
    let established = shared.established.load(Relaxed);
    let fatal = (!interrupted && established == 0).then(|| setup_failure(&shared, connections));

    if fatal.is_none() && !interrupted {
        publish_start_nanos = Some(shared.elapsed_nanos());
        tokio::select! {
            () = publisher::run(&shared, publisher, args.rate, args.duration(), args.payload_bytes) => {}
            _ = &mut ctrl_c => interrupted = true,
        }
    }

    if fatal.is_none() && !interrupted {
        let deadline = Instant::now() + args.drain_timeout();
        while shared.received.load(Relaxed) < expected(&shared) && Instant::now() < deadline {
            tokio::select! {
                () = sleep(DRAIN_POLL) => {}
                _ = &mut ctrl_c => {
                    interrupted = true;
                    break;
                }
            }
        }
    }

    let _ = stop_tx.send(true);
    let _ = timeout(TEARDOWN_TIMEOUT, alive_rx.recv()).await;

    if let Some(message) = fatal {
        bail!(message);
    }
    Ok(shared.report(
        &args,
        &RunFacts {
            connect_elapsed,
            publish_start_nanos,
            interrupted,
        },
    ))
}

fn expected(shared: &Shared) -> u64 {
    shared
        .published_ok
        .load(Relaxed)
        .saturating_mul(shared.established.load(Relaxed))
}

fn setup_failure(shared: &Shared, requested: usize) -> String {
    let mut message = format!("no connection could be established (0 of {requested})");
    if let Some(first) = shared.first_connect_error() {
        message.push_str(": ");
        message.push_str(&first);
    }
    let fd_failures = shared.fd_exhausted();
    if fd_failures > 0 {
        message.push('\n');
        message.push_str(&fd_hint(fd_failures));
    }
    message
}
