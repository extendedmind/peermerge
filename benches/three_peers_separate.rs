use std::time::Instant;

use criterion::async_executor::AsyncStdExecutor;
use criterion::Criterion;
use criterion::{black_box, criterion_group, criterion_main};
use futures::channel::mpsc::{Receiver, Sender, UnboundedReceiver, UnboundedSender};
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
    let mut peers_synced: usize = 0;
    let mut document_changed: usize = 0;
    while let Some(event) = receiver.next().await {
        match event {
            StateEvent::PeerSynced(_) => {
                peers_synced += 1;
                if peers_synced == 6 && document_changed == 8 {
                    break;
                }
            }
            StateEvent::DocumentChanged(_) => {
                document_changed += 1;
                if peers_synced == 6 && document_changed == 8 {
                    break;
                }
            }
            _ => {}
        }
    }
    0
}

fn bench_setup_mesh_of_three(c: &mut Criterion) {
    let mut group = c.benchmark_group("slow_call");
    group.bench_function("mesh_of_three", move |b| {
        b.to_async(AsyncStdExecutor)
            .iter_custom(|iters| async move {
                println!("ITERING {}", iters);
                let start = Instant::now();
                for _ in 0..iters {
                    black_box(setup_hypermerge_mesh(3).await);
                }
                start.elapsed()
            });
    });
    group.finish();
}

fn bench_append_three(c: &mut Criterion) {
    let mut group = c.benchmark_group("slow_call");
    group.bench_function("append_three", move |b| {
        b.to_async(AsyncStdExecutor)
            .iter_custom(|iters| async move {
                tracing_subscriber::fmt()
                    // .with_max_level(tracing::Level::DEBUG)
                    .init();
                println!("ITERING {}", iters);
                let (senders, mut receiver) = setup_hypermerge_mesh(3).await;
                println!("CREATED");
                let start = Instant::now();
                for i in 0..iters {
                    black_box(append_three(i, senders.clone(), &mut receiver).await);
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
    targets = bench_setup_mesh_of_three/*, bench_append_three*/
}
