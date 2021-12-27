use futures_lite::{stream, AsyncWriteExt, StreamExt};
use glommio::{
    io::{DmaFile, ImmutableFile, ImmutableFileBuilder},
    LocalExecutorBuilder,
};
use rand::Rng;
use std::{
    path::PathBuf,
    rc::Rc,
    time::{Duration, Instant},
};

fn main() {
    let handle = LocalExecutorBuilder::default()
        .preempt_timer(Duration::from_secs(1))
        .spin_before_park(Duration::from_millis(100))
        .spawn(|| async move {
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

                run_bench("immutable file", &file, 1_000_000, 4094).await;
            };

            {
                let file = Rc::new(glommio::io::DmaFile::open(&filename).await.unwrap());
                run_bench2("mutable file", &file, 1_000_000, 4094).await;
            }
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
    let mut hist = hdrhistogram::Histogram::<u64>::new(4).unwrap();
    let started_at = Instant::now();

    file.read_many(
        stream::repeat_with(|| IOVec {
            vec: (rand.gen_range(0..file.file_size() - size as u64), size),
            at: Instant::now(),
        })
        .take(count),
        0,
        Some(0),
    )
    .for_each(|res| {
        let (io, _) = res.unwrap();
        hist.record(io.at.elapsed().as_nanos() as u64).unwrap();
    })
    .await;

    println!("\n --- {} ---", name);
    println!(
        "performed {} read IO at {} IOPS (took {:.2}s)",
        count,
        (count as f64 / started_at.elapsed().as_secs_f64()) as usize,
        started_at.elapsed().as_secs_f64()
    );
    println!(
        "latencies:\tp50: {}us\tp99: {}us\tp99.9: {}us\tp99.99: {}us",
        hist.value_at_percentile(0.5),
        hist.value_at_percentile(0.99),
        hist.value_at_percentile(0.999),
        hist.value_at_percentile(0.9999),
    );
}

async fn run_bench2(name: &str, file: &Rc<DmaFile>, count: usize, size: usize) {
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

    let file_size = file.file_size().await.unwrap();
    let mut rand = rand::thread_rng();
    let mut hist = hdrhistogram::Histogram::<u64>::new(4).unwrap();
    let started_at = Instant::now();

    file.read_many(
        stream::repeat_with(|| IOVec {
            vec: (rand.gen_range(0..file_size - size as u64), size),
            at: Instant::now(),
        })
        .take(count),
        0,
        Some(0),
    )
    .for_each(|res| {
        let (io, _) = res.unwrap();
        hist.record(io.at.elapsed().as_nanos() as u64).unwrap();
    })
    .await;

    println!("\n --- {} ---", name);
    println!(
        "performed {} read IO at {} IOPS (took {:.2}s)",
        count,
        (count as f64 / started_at.elapsed().as_secs_f64()) as usize,
        started_at.elapsed().as_secs_f64()
    );
    println!(
        "latencies:\tp50: {}us\tp99: {}us\tp99.9: {}us\tp99.99: {}us",
        hist.value_at_percentile(0.5),
        hist.value_at_percentile(0.99),
        hist.value_at_percentile(0.999),
        hist.value_at_percentile(0.9999),
    );
}
