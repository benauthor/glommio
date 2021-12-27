use futures_lite::{stream, AsyncWriteExt, StreamExt};
use glommio::{
    io::{ImmutableFile, ImmutableFileBuilder},
    LocalExecutorBuilder,
    Placement,
};
use rand::Rng;
use std::{
    path::PathBuf,
    time::{Duration, Instant},
};

fn main() {
    let handle = LocalExecutorBuilder::new(Placement::Fixed(0))
        // .spin_before_park(std::time::Duration::from_secs(1))
        .spawn(|| async move {
            // glommio::spawn_local(async move {
            let filename = match std::env::var("GLOMMIO_TEST_POLLIO_ROOTDIR") {
                Ok(path) => {
                    let mut path = PathBuf::from(path);
                    path.push("benchfile");
                    path
                }
                Err(_) => panic!("please set 'GLOMMIO_TEST_POLLIO_ROOTDIR'"),
            };

            {
                let _ = std::fs::remove_file(&filename);
                let mut sink = ImmutableFileBuilder::new(&filename)
                    .with_buffer_size(512 << 10)
                    .with_pre_allocation(Some(64 << 20))
                    .build_sink()
                    .await
                    .unwrap();

                let contents = vec![1; 512 << 10];
                for _ in 0..(64 << 20) / contents.len() {
                    sink.write_all(&contents).await.unwrap();
                }
                let file = sink.seal().await.unwrap();

                for x in 0..4 {
                    run_bench(&format!("iteration: {}", x), &file, 1_000_000, 4096).await;
                }
            };
            // })
            // .await;
        })
        .unwrap();
    handle.join().unwrap();
}

async fn run_bench(name: &str, file: &ImmutableFile, count: usize, size: usize) {
    struct IOVec<T: glommio::io::IoVec> {
        vec: T,
        at: Instant,
    }

    impl<T: glommio::io::IoVec> glommio::io::IoVec for IOVec<T> {
        fn pos(&self) -> u64 {
            self.vec.pos()
        }

        fn size(&self) -> usize {
            self.vec.size()
        }
    }

    let mut rand = rand::thread_rng();
    let blocks = file.file_size() / size as u64 - 1;
    let mut hist =
        sketches_ddsketch::DDSketch::new(sketches_ddsketch::Config::new(0.01, 2048, 1.0e-9));
    let started_at = Instant::now();

    file.read_many(
        stream::repeat_with(|| IOVec {
            vec: (rand.gen_range(0..blocks) * size as u64, size),
            at: Instant::now(),
        })
        .take(count),
        0,
        Some(0),
    )
    .with_concurrency(128)
    .for_each(|res| {
        let (io, _) = res.unwrap();
        hist.add(io.at.elapsed().as_micros() as f64);
    })
    .await;

    assert_eq!(hist.count() as usize, count);

    println!("\n --- {} ---", name);
    println!(
        "performed {} read IO at {} IOPS (took {:.2}s)",
        count,
        (count as f64 / started_at.elapsed().as_secs_f64()) as usize,
        started_at.elapsed().as_secs_f64()
    );
    println!(
        "[measured] lat (end-to-end):\t\t\t\t\t\tmin: {: >4}us\tp50: {: >4}us\tp99: {: \
         >4}us\tp99.9: {: >4}us\tp99.99: {: >4}us\tp99.999: {: >4}us\t max: {: >4}us",
        Duration::from_micros(hist.min().unwrap() as u64).as_micros(),
        Duration::from_micros(hist.quantile(0.5).unwrap().unwrap() as u64).as_micros(),
        Duration::from_micros(hist.quantile(0.99).unwrap().unwrap() as u64).as_micros(),
        Duration::from_micros(hist.quantile(0.999).unwrap().unwrap() as u64).as_micros(),
        Duration::from_micros(hist.quantile(0.9999).unwrap().unwrap() as u64).as_micros(),
        Duration::from_micros(hist.quantile(0.99999).unwrap().unwrap() as u64).as_micros(),
        Duration::from_micros(hist.max().unwrap() as u64).as_micros(),
    );

    let io_stats = glommio::executor().io_stats().all_rings();

    let sk = io_stats.post_reactor_io_scheduler_latency_us().clone();
    println!(
        "[measured] sched lat (from fulfilled to consumed):\tmin: {: >4}us\tp50: {: >4}us\tp99: \
         {: >4}us\tp99.9: {: >4}us\tp99.99: {: >4}us\tp99.999: {: >4}us\t max: {: >4}us",
        Duration::from_micros(sk.min().unwrap_or_default() as u64).as_micros(),
        Duration::from_micros(sk.quantile(0.5).unwrap().unwrap_or_default() as u64).as_micros(),
        Duration::from_micros(sk.quantile(0.99).unwrap().unwrap_or_default() as u64).as_micros(),
        Duration::from_micros(sk.quantile(0.999).unwrap().unwrap_or_default() as u64).as_micros(),
        Duration::from_micros(sk.quantile(0.9999).unwrap().unwrap_or_default() as u64).as_micros(),
        Duration::from_micros(sk.quantile(0.99999).unwrap().unwrap_or_default() as u64).as_micros(),
        Duration::from_micros(sk.max().unwrap_or_default() as u64).as_micros(),
    );

    let sk = io_stats.io_latency_us().clone();
    println!(
        "[measured] IO lat (time in ring):\t\t\t\t\tmin: {: >4}us\tp50: {: >4}us\tp99: {: \
         >4}us\tp99.9: {: >4}us\tp99.99: {: >4}us\tp99.999: {: >4}us\t max: {: >4}us",
        Duration::from_micros(sk.min().unwrap_or_default() as u64).as_micros(),
        Duration::from_micros(sk.quantile(0.5).unwrap().unwrap_or_default() as u64).as_micros(),
        Duration::from_micros(sk.quantile(0.99).unwrap().unwrap_or_default() as u64).as_micros(),
        Duration::from_micros(sk.quantile(0.999).unwrap().unwrap_or_default() as u64).as_micros(),
        Duration::from_micros(sk.quantile(0.9999).unwrap().unwrap_or_default() as u64).as_micros(),
        Duration::from_micros(sk.quantile(0.99999).unwrap().unwrap_or_default() as u64).as_micros(),
        Duration::from_micros(sk.max().unwrap_or_default() as u64).as_micros(),
    );
}
