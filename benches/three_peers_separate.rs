use std::time::Instant;

use criterion::async_executor::AsyncStdExecutor;
use criterion::Criterion;
use criterion::{black_box, criterion_group, criterion_main};
use futures::channel::mpsc::{Sender, UnboundedReceiver};
use futures::stream::StreamExt;
use hypermerge::StateEvent;
use pprof::criterion::{Output, PProfProfiler};

mod common;
use common::setup_hypermerge_mesh;

async fn append_three(
    i: u64,
    senders: Vec<Sender<u64>>,
    receiver: &mut UnboundedReceiver<StateEvent>,
) -> u64 {
    for mut sender in senders {
        sender.try_send(i).unwrap();
    }
    let mut sync_remaining = 6;
    let mut remote_sync_remaining = 6;
    let mut document_changed_remaining = 9;

    while let Some(event) = receiver.next().await {
        match event {
            StateEvent::PeerSynced(_) => {
                sync_remaining -= 1;
                // println!(
                //     "PS i={} sr={}, rsr={}, dcr={}",
                //     i, sync_remaining, remote_sync_remaining, document_changed_remaining
                // );
                if sync_remaining == 0
                    && remote_sync_remaining == 0
                    && document_changed_remaining == 0
                {
                    break;
                }
            }
            StateEvent::DocumentChanged(_) => {
                document_changed_remaining -= 1;
                // TODO: Sometimes one DC is missing
                // println!(
                //     "DC: i={} sr={}, rsr={}, dcr={}",
                //     i, sync_remaining, remote_sync_remaining, document_changed_remaining
                // );
                if sync_remaining == 0
                    && remote_sync_remaining == 0
                    && document_changed_remaining == 0
                {
                    break;
                }
            }
            StateEvent::RemotePeerSynced(..) => {
                remote_sync_remaining -= 1;
                // TODO: Sometimes there is one extra RPS
                // println!(
                //     "RPS: i={} sr={}, rsr={}, dcr={}",
                //     i, sync_remaining, remote_sync_remaining, document_changed_remaining
                // );
                if sync_remaining == 0
                    && remote_sync_remaining == 0
                    && document_changed_remaining == 0
                {
                    break;
                }
            }
        }
    }
    0
}

fn bench_setup_mesh_of_three_plain(c: &mut Criterion) {
    bench_setup_mesh_of_three(c, false);
}

fn bench_setup_mesh_of_three_encrypted(c: &mut Criterion) {
    bench_setup_mesh_of_three(c, true);
}

fn bench_setup_mesh_of_three(c: &mut Criterion, encrypted: bool) {
    let name = format!(
        "mesh_of_three_{}",
        if encrypted { "plain" } else { "encrypted" }
    );
    let mut group = c.benchmark_group("slow_call");
    group.bench_function(name, move |b| {
        b.to_async(AsyncStdExecutor)
            .iter_custom(|iters| async move {
                // println!("MESH ITERING {}", iters);
                // tracing_subscriber::fmt()
                //     .with_max_level(tracing::Level::DEBUG)
                //     .init();

                let start = Instant::now();
                for _ in 0..iters {
                    black_box(setup_hypermerge_mesh(3, encrypted).await);
                }
                start.elapsed()
            });
    });
    group.finish();
}

fn bench_append_three_plain(c: &mut Criterion) {
    bench_append_three(c, false);
}

fn bench_append_three_encrypted(c: &mut Criterion) {
    bench_append_three(c, true);
}

fn bench_append_three(c: &mut Criterion, encrypted: bool) {
    let mut group = c.benchmark_group("slow_call");
    let name = format!(
        "append_three_{}",
        if encrypted { "plain" } else { "encrypted" }
    );
    group.bench_function(name, move |b| {
        b.to_async(AsyncStdExecutor)
            .iter_custom(|iters| async move {
                // println!("APPEND ITERING {}", iters);
                // tracing_subscriber::fmt()
                //     .with_max_level(tracing::Level::DEBUG)
                //     .try_init()
                //     .ok();
                let (senders, mut receiver) = setup_hypermerge_mesh(3, encrypted).await;
                // async_std::task::sleep(std::time::Duration::from_millis(100)).await;
                let start = Instant::now();
                for i in 0..iters {
                    black_box(append_three(i, senders.clone(), &mut receiver).await);
                    // println!("APPEND ITERING {} READY {}", iters, i);
                }
                start.elapsed()
            });
    });
    group.finish();
}

criterion_main!(benches);
criterion_group! {
    name = benches;
    config = Criterion::default()
        .with_profiler(
            PProfProfiler::new(100, Output::Flamegraph(None))
        );
    targets = bench_setup_mesh_of_three_plain, bench_append_three_plain, bench_setup_mesh_of_three_encrypted, bench_append_three_encrypted,
}
