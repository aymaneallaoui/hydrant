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
    time::{Instant, MissedTickBehavior, interval, sleep, timeout},
};

use crate::{
    cli::Args,
    publisher::Publisher,
    stats::{Report, RunFacts, Shared, fd_hint},
    subscriber::{Ctx, Job},
};

const DRAIN_POLL: Duration = Duration::from_millis(5);
const PROGRESS_EVERY: Duration = Duration::from_secs(1);
const TEARDOWN_TIMEOUT: Duration = Duration::from_secs(10);

pub async fn run(args: Args) -> anyhow::Result<Report> {
    payload::init_clock();
    let mut ctrl_c = pin!(tokio::signal::ctrl_c());
    let target = tokio::select! {
        target = cli::resolve_target(&args.url) => target?,
        _ = &mut ctrl_c => bail!("interrupted before the run started"),
    };
    let connections = args.connections()?;
    let targets = cli::spread_targets(&target, args.spread_loopback)?;
    let shared = Arc::new(Shared::new());
    let mut interrupted = false;

    let (stop_tx, stop_rx) = watch::channel(false);
    let (alive_tx, mut alive_rx) = mpsc::channel::<()>(1);
    let (connected_tx, mut connected_rx) = mpsc::unbounded_channel();
    let ctx = Arc::new(Ctx {
        shared: Arc::clone(&shared),
        targets,
        topic: args.topic.clone(),
        payload_bytes: args.payload_bytes,
        transport: args.transport,
        connected: connected_tx,
    });
    let permits = Arc::new(Semaphore::new(args.connect_concurrency()?));

    let mut publisher = Publisher::new(target, &args.topic);
    tokio::select! {
        connected = timeout(subscriber::CONNECT_TIMEOUT, publisher.connect()) => connected
            .context("connecting the publisher connection timed out")?
            .context("connecting the publisher connection")?,
        _ = &mut ctrl_c => bail!("interrupted before the run started"),
    }

    let connect_started = Instant::now();
    let progress = tokio::spawn(report_progress(Arc::clone(&shared), connections));
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
    progress.abort();
    let connect_elapsed = connect_started.elapsed();

    let established = shared.established.load(Relaxed);
    let fatal = (!interrupted && established == 0).then(|| setup_failure(&shared, connections));

    if fatal.is_none() && !interrupted {
        eprintln!(
            "loadgen: all connected: established={established} failed={} in {:.3}s",
            shared.connect_failed.load(Relaxed),
            connect_elapsed.as_secs_f64()
        );
        tokio::select! {
            () = hold_then_publish(&shared, publisher, &args) => {}
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

    eprintln!("loadgen: disconnecting");
    let _ = stop_tx.send(true);
    let _ = timeout(TEARDOWN_TIMEOUT, alive_rx.recv()).await;

    if let Some(message) = fatal {
        bail!(message);
    }
    Ok(shared.report(
        &args,
        &RunFacts {
            connect_elapsed,
            interrupted,
        },
    ))
}

async fn hold_then_publish(shared: &Shared, publisher: Publisher, args: &Args) {
    sleep(args.hold_idle()).await;
    eprintln!("loadgen: publish starting");
    publisher::run(
        shared,
        publisher,
        args.rate,
        args.duration(),
        args.payload_bytes,
    )
    .await;
}

async fn report_progress(shared: Arc<Shared>, requested: usize) {
    let mut tick = interval(PROGRESS_EVERY);
    tick.set_missed_tick_behavior(MissedTickBehavior::Skip);
    tick.tick().await;
    loop {
        tick.tick().await;
        eprintln!(
            "loadgen: connecting: established={} failed={} of {requested}",
            shared.established.load(Relaxed),
            shared.connect_failed.load(Relaxed)
        );
    }
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
