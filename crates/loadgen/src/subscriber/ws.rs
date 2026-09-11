use std::time::Duration;

use futures_util::StreamExt;
use tokio::{net::TcpStream, time::timeout};
use tokio_tungstenite::{
    WebSocketStream, client_async_with_config,
    tungstenite::{
        Error as WsError, Message,
        protocol::{WebSocketConfig, frame::coding::CloseCode},
    },
};

use super::{CONNECT_TIMEOUT, ConnectFailure, Ctx, EndReason, Job, stopped};
use crate::stats::SeqTracker;

const READ_BUFFER_BYTES: usize = 4096;
const CLOSE_GRACE: Duration = Duration::from_secs(2);

pub async fn run(job: Job) {
    let Job {
        ctx,
        index,
        permit,
        mut stop,
        alive: _alive,
    } = job;

    let connected = timeout(CONNECT_TIMEOUT, connect(&ctx))
        .await
        .unwrap_or_else(|_| Err(ConnectFailure::new("connect timed out")));
    drop(permit);
    let mut ws = match connected {
        Ok(ws) => {
            ctx.connect_succeeded();
            ws
        }
        Err(failure) => {
            ctx.connect_failed(&failure);
            return;
        }
    };

    let mut seqs = SeqTracker::default();
    loop {
        tokio::select! {
            biased;
            () = stopped(&mut stop) => {
                polite_close(&mut ws).await;
                return;
            }
            frame = ws.next() => {
                let reason = match frame {
                    Some(Ok(Message::Text(text))) => {
                        ctx.shared.record_payload(index, &mut seqs, text.as_bytes());
                        continue;
                    }
                    Some(Ok(Message::Close(frame))) => {
                        EndReason::WsClose(frame.map(|f| close_code(f.code)))
                    }
                    Some(Ok(_)) => continue,
                    Some(Err(err)) => EndReason::Error(err.to_string()),
                    None => EndReason::Eof,
                };
                ctx.shared.record_closed_early(&reason);
                polite_close(&mut ws).await;
                return;
            }
        }
    }
}

async fn connect(ctx: &Ctx) -> Result<WebSocketStream<TcpStream>, ConnectFailure> {
    let tcp = TcpStream::connect(ctx.target.addr)
        .await
        .map_err(|err| ConnectFailure::from_io(&err))?;
    let url = format!("ws://{}/topics/{}/ws", ctx.target.authority, ctx.topic);
    let config = WebSocketConfig::default()
        .read_buffer_size(READ_BUFFER_BYTES)
        .write_buffer_size(0);
    let (ws, _response) = client_async_with_config(url, tcp, Some(config))
        .await
        .map_err(|err| match err {
            WsError::Io(io) => ConnectFailure::from_io(&io),
            WsError::Http(response) => {
                ConnectFailure::new(format!("handshake rejected: HTTP {}", response.status()))
            }
            other => ConnectFailure::new(other.to_string()),
        })?;
    Ok(ws)
}

async fn polite_close(ws: &mut WebSocketStream<TcpStream>) {
    let _ = timeout(CLOSE_GRACE, async {
        let _ = ws.close(None).await;
        while let Some(Ok(_)) = ws.next().await {}
    })
    .await;
}

fn close_code(code: CloseCode) -> u16 {
    u16::from(code)
}
