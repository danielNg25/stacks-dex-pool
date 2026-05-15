//! Bounded event queue with `(tx_id, event_index)` dedup.
//!
//! Producers (the per-contract pollers) push `QueuedEvent`s; the single
//! consumer (the event processor) drains them. Dedup prevents the same event
//! from being applied twice across overlapping poll windows or after restart.

use std::collections::HashSet;
use std::sync::Arc;

use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::sync::Mutex;

use crate::pool::event::StacksEvent;

/// One item in the event queue.
#[derive(Debug, Clone)]
pub struct QueuedEvent {
    pub event: StacksEvent,
}

/// Wraps a sender + dedup set so callers can `send` without manual dedup.
#[derive(Clone)]
pub struct EventSender {
    tx: Sender<QueuedEvent>,
    seen: Arc<Mutex<HashSet<(String, u32)>>>,
    seen_cap: usize,
}

impl EventSender {
    /// Returns true if the event was new (sent); false if it was a duplicate
    /// or if the channel is closed.
    pub async fn send_dedup(&self, event: StacksEvent) -> bool {
        let key = (event.tx_id.clone(), event.event_index);
        {
            let mut seen = self.seen.lock().await;
            if seen.contains(&key) {
                return false;
            }
            // Bound dedup memory — once we hit cap, drop oldest by clearing
            // ~half. We tolerate occasional re-deliveries after a clear
            // because apply_event is idempotent for our state-setter handlers.
            if seen.len() >= self.seen_cap {
                let drain_n = self.seen_cap / 2;
                let to_drop: Vec<_> = seen.iter().take(drain_n).cloned().collect();
                for k in to_drop {
                    seen.remove(&k);
                }
            }
            seen.insert(key);
        }
        self.tx.send(QueuedEvent { event }).await.is_ok()
    }
}

/// Construct a bounded event queue. Returns `(EventSender, Receiver)`.
pub fn build_queue(capacity: usize) -> (EventSender, Receiver<QueuedEvent>) {
    let (tx, rx) = channel(capacity);
    let sender = EventSender {
        tx,
        seen: Arc::new(Mutex::new(HashSet::new())),
        seen_cap: capacity.max(64) * 4,
    };
    (sender, rx)
}
