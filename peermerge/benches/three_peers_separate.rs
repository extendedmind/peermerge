use std::time::{Duration, Instant};

use criterion::Criterion;
use criterion::{black_box, criterion_group, criterion_main};
use futures::channel::mpsc::{Sender, UnboundedReceiver};
use futures::stream::StreamExt;
use peermerge::{StateEvent, StateEventContent::*};
use pprof::criterion::{Output, PProfProfiler};

#[cfg(feature = "async-std")]
use criterion::async_executor::AsyncStdExecutor;

mod common;
use common::{setup_peermerge_mesh_disk, setup_peermerge_mesh_memory};

async fn append_three(
    i: u64,
    senders: Vec<Sender<u64>>,
    receiver: &mut UnboundedReceiver<StateEvent>,
) -> u64 {
    // println!("");
    // println!("====");
    for mut sender in senders {
        sender.try_send(i).unwrap();
    }
    let mut sync_remaining = 6;
    let mut remote_sync_remaining = 6;
    let mut patches_remaining: i64 = 9;

    while let Some(event) = receiver.next().await {
        match event.content {
            PeerSynced(_) => {
                sync_remaining -= 1;
                // println!(
                //     "PS i={} sr={}, rsr={}, pr={}",
                //     i, sync_remaining, remote_sync_remaining, patches_remaining
                // );
                if sync_remaining == 0 && remote_sync_remaining == 0 && patches_remaining == 0 {
                    break;
                }
            }
            DocumentChanged(patches) => {
                patches_remaining -= patches.len() as i64;
                // println!(
                //     "DC: i={} sr={}, rsr={}, pr={}",
                //     i, sync_remaining, remote_sync_remaining, patches_remaining
                // );
                if sync_remaining == 0 && remote_sync_remaining == 0 && patches_remaining == 0 {
                    break;
                }
            }
            RemotePeerSynced(..) => {
                remote_sync_remaining -= 1;
                // println!(
                //     "RPS: i={} sr={}, rsr={}, pr={}",
                //     i, sync_remaining, remote_sync_remaining, patches_remaining
                // );

                if sync_remaining == 0 && remote_sync_remaining == 0 && patches_remaining == 0 {
                    break;
                }
            }
            DocumentInitialized() => {
                panic!("Should not get document initialized");
            }
            Reattached(_) => {
                panic!("Should not get reattached");
            }
        }
        if sync_remaining < 0 && remote_sync_remaining < 0 && patches_remaining < 0 {
            panic!(
                "Too many events: i={} sr={}, rsr={}, pr={}",
                i, sync_remaining, remote_sync_remaining, patches_remaining
            );
        }
    }
    0
}

fn bench_setup_mesh_of_three_memory_plain(c: &mut Criterion) {
    bench_setup_mesh_of_three_memory(c, false);
}

fn bench_setup_mesh_of_three_memory_encrypted(c: &mut Criterion) {
    bench_setup_mesh_of_three_memory(c, true);
}

fn bench_setup_mesh_of_three_memory(c: &mut Criterion, encrypted: bool) {
    let name = format!(
        "mesh_of_three_memory_{}",
        if encrypted { "encrypted" } else { "plain" }
    );
    let mut group = c.benchmark_group("slow_call");
    group.measurement_time(Duration::from_secs(10));
    #[cfg(feature = "async-std")]
    group.bench_function(name, move |b| {
        b.to_async(AsyncStdExecutor)
            .iter_custom(|iters| async move {
                bench_setup_mesh_of_three_memory_iters(iters, encrypted).await
            });
    });
    #[cfg(feature = "tokio")]
    group.bench_function(name, move |b| {
        let rt = tokio::runtime::Runtime::new().unwrap();
        b.to_async(&rt).iter_custom(|iters| async move {
            bench_setup_mesh_of_three_memory_iters(iters, encrypted).await
        });
    });
    group.finish();
}

async fn bench_setup_mesh_of_three_memory_iters(iters: u64, encrypted: bool) -> Duration {
    // println!("MESH ITERING {}", iters);
    // tracing_subscriber::fmt()
    //     .with_max_level(tracing::Level::DEBUG)
    //     .try_init()
    //     .ok();

    let start = Instant::now();
    for _ in 0..iters {
        black_box(setup_peermerge_mesh_memory(3, encrypted).await);
    }
    start.elapsed()
}

fn bench_append_three_memory_plain(c: &mut Criterion) {
    bench_append_three_memory(c, false);
}

fn bench_append_three_memory_encrypted(c: &mut Criterion) {
    bench_append_three_memory(c, true);
}

fn bench_append_three_memory(c: &mut Criterion, encrypted: bool) {
    let mut group = c.benchmark_group("slow_call");
    group.measurement_time(Duration::from_secs(10));
    let name = format!(
        "append_three_memory_{}",
        if encrypted { "encrypted" } else { "plain" }
    );

    #[cfg(feature = "async-std")]
    group.bench_function(name, move |b| {
        b.to_async(AsyncStdExecutor)
            .iter_custom(
                |iters| async move { bench_append_three_memory_iters(iters, encrypted).await },
            );
    });
    #[cfg(feature = "tokio")]
    group.bench_function(name, move |b| {
        let rt = tokio::runtime::Runtime::new().unwrap();
        b.to_async(&rt).iter_custom(|iters| async move {
            bench_append_three_memory_iters(iters, encrypted).await
        });
    });

    group.finish();
}

async fn bench_append_three_memory_iters(iters: u64, encrypted: bool) -> Duration {
    // println!("APPEND ITERING {}", iters);
    // tracing_subscriber::fmt()
    //     .with_max_level(tracing::Level::DEBUG)
    //     .try_init()
    //     .ok();
    let (senders, mut receiver) = setup_peermerge_mesh_memory(3, encrypted).await;
    // async_std::task::sleep(std::time::Duration::from_millis(100)).await;
    let start = Instant::now();
    for i in 0..iters {
        black_box(append_three(i, senders.clone(), &mut receiver).await);
        // println!("APPEND ITERING {} READY {}", iters, i);
    }
    start.elapsed()
}

fn bench_append_three_disk_encrypted(c: &mut Criterion) {
    bench_append_three_disk(c, true);
}

fn bench_append_three_disk(c: &mut Criterion, encrypted: bool) {
    let mut group = c.benchmark_group("very_slow_call");
    group.measurement_time(Duration::from_secs(500));
    let name = format!(
        "append_three_disk_{}",
        if encrypted { "encrypted" } else { "plain" }
    );

    #[cfg(feature = "async-std")]
    group.bench_function(name, move |b| {
        b.to_async(AsyncStdExecutor)
            .iter_custom(
                |iters| async move { bench_append_three_disk_iters(iters, encrypted).await },
            );
    });
    #[cfg(feature = "tokio")]
    group.bench_function(name, move |b| {
        let rt = tokio::runtime::Runtime::new().unwrap();
        b.to_async(&rt).iter_custom(|iters| async move {
            bench_append_three_disk_iters(iters, encrypted).await
        });
    });

    group.finish();
}

async fn bench_append_three_disk_iters(iters: u64, encrypted: bool) -> Duration {
    let (senders, mut receiver) = setup_peermerge_mesh_disk(3, encrypted).await;
    let start = Instant::now();
    for i in 0..iters {
        black_box(append_three(i, senders.clone(), &mut receiver).await);
    }
    start.elapsed()
}

criterion_main!(benches);
criterion_group! {
    name = benches;
    config = Criterion::default()
        .with_profiler(
            PProfProfiler::new(100, Output::Flamegraph(None))
        );
    targets = bench_setup_mesh_of_three_memory_plain, bench_setup_mesh_of_three_memory_encrypted, bench_append_three_memory_plain, bench_append_three_memory_encrypted, bench_append_three_disk_encrypted
}
