use hypercore_protocol::{Duplex, Protocol, ProtocolBuilder};
use peermerge::{
    automerge::{AutomergeError, ObjId, Prop, ReadDoc, ScalarValue, Value},
    AutomergeDoc,
};
use rand::distributions::{Alphanumeric, DistString};
use rand::rngs::StdRng;
use rand::SeedableRng;
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

pub fn get_id<O: AsRef<ObjId>, P: Into<Prop>>(
    doc: &AutomergeDoc,
    obj: O,
    prop: P,
) -> Result<Option<ObjId>, AutomergeError> {
    let result = doc.get(obj, prop)?.map(|result| result.1);
    Ok(result)
}

pub fn get_scalar<O: AsRef<ObjId>, P: Into<Prop>>(
    doc: &AutomergeDoc,
    obj: O,
    prop: P,
) -> Result<Option<ScalarValue>, AutomergeError> {
    let result = doc
        .get(obj, prop)?
        .and_then(|result| result.0.to_scalar().cloned());
    Ok(result)
}

pub fn get<O: AsRef<ObjId>, P: Into<Prop>>(
    doc: &AutomergeDoc,
    obj: O,
    prop: P,
) -> Result<Option<(Value, ObjId)>, AutomergeError> {
    let result = doc
        .get(obj, prop)?
        .map(|(value, id)| (value.to_owned(), id));
    Ok(result)
}

pub fn realize_text<O: AsRef<ObjId>>(
    doc: &AutomergeDoc,
    obj: O,
) -> Result<Option<String>, AutomergeError> {
    let length = doc.length(obj.as_ref().clone());
    let mut chars = Vec::with_capacity(length);
    for i in 0..length {
        match doc.get(obj.as_ref().clone(), i) {
            Ok(result) => {
                if let Some(result) = result {
                    let scalar = result.0.to_scalar().unwrap();
                    match scalar {
                        ScalarValue::Str(character) => {
                            chars.push(character.to_string());
                        }
                        _ => {
                            panic!("Not a char")
                        }
                    }
                }
            }
            Err(_err) => {
                panic!("Not a char")
            }
        };
    }
    let string: String = chars.into_iter().collect();
    Ok(Some(string))
}

pub fn generate_string(seed: u64, length: usize) -> String {
    let mut seeded_rng = StdRng::seed_from_u64(seed);
    Alphanumeric.sample_string(&mut seeded_rng, length)
}
