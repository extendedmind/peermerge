use hypercore_protocol::{Duplex, Protocol, ProtocolBuilder};
use std::sync::Arc;

#[cfg(feature = "async-std")]
use async_std::sync::{Condvar, Mutex};
#[cfg(feature = "tokio")]
use tokio::sync::{Mutex, Notify};

pub type MemoryProtocol = Protocol<Duplex<sluice::pipe::PipeReader, sluice::pipe::PipeWriter>>;
pub async fn create_pair_memory() -> (MemoryProtocol, MemoryProtocol) {
    let (ar, bw) = sluice::pipe::pipe();
    let (br, aw) = sluice::pipe::pipe();

    let responder = ProtocolBuilder::new(false);
    let initiator = ProtocolBuilder::new(true);
    let responder = responder.connect_rw(ar, aw);
    let initiator = initiator.connect_rw(br, bw);
    (responder, initiator)
}

#[cfg(feature = "async-std")]
pub type BoolCondvar = Arc<(Mutex<bool>, Condvar)>;

#[cfg(feature = "tokio")]
pub type BoolCondvar = Arc<(Mutex<bool>, Notify)>;

#[cfg(feature = "async-std")]
pub fn init_condvar() -> BoolCondvar {
    Arc::new((Mutex::new(false), Condvar::new()))
}

#[cfg(feature = "tokio")]
pub fn init_condvar() -> BoolCondvar {
    Arc::new((Mutex::new(false), Notify::new()))
}

#[cfg(feature = "async-std")]
pub async fn wait_for_condvar(sync: BoolCondvar) {
    let (lock, cvar) = &*sync;
    let mut guard = lock.lock().await;
    while !*guard {
        guard = cvar.wait(guard).await;
    }
}

#[cfg(feature = "tokio")]
pub async fn wait_for_condvar(sync: BoolCondvar) {
    let (lock, notify) = &*sync;
    loop {
        let future = notify.notified();
        {
            let guard = lock.lock().await;
            if *guard {
                return;
            }
        }
        future.await;
    }
}

#[cfg(feature = "async-std")]
pub async fn notify_all_condvar(sync: BoolCondvar) {
    let (lock, cvar) = &*sync;
    let mut guard = lock.lock().await;
    *guard = true;
    cvar.notify_all();
}

#[cfg(feature = "tokio")]
pub async fn notify_all_condvar(sync: BoolCondvar) {
    let (lock, notify) = &*sync;
    let mut guard = lock.lock().await;
    *guard = true;
    notify.notify_waiters();
}

#[cfg(feature = "async-std")]
pub async fn notify_one_condvar(sync: BoolCondvar) {
    let (lock, notify) = &*sync;
    let mut guard = lock.lock().await;
    *guard = true;
    notify.notify_one();
}

#[cfg(feature = "tokio")]
pub async fn notify_one_condvar(sync: BoolCondvar) {
    let (lock, cvar) = &*sync;
    let mut guard = lock.lock().await;
    *guard = true;
    cvar.notify_one();
}
