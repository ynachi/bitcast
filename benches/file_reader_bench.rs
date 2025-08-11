use crc_fast::CrcAlgorithm::Crc32IsoHdlc;
use crc_fast::checksum;
use criterion::{BenchmarkId, Criterion, Throughput, black_box, criterion_group, criterion_main};
use std::fs::File;
use std::io::Write;
use std::path::PathBuf;
use tempfile::TempDir;

// Import your crate items
use ybitcast::{ByteRange, EngineOptions, FileReader, ReaderOptions};

// Benchmark configuration
const BENCH_KEY_MAX_SIZE: usize = 4096;
const BENCH_VALUE_MAX_SIZE: usize = 1024 * 1024; // 1MB
const BENCH_FILE_ID: u64 = 42;
const BENCH_TIMESTAMP: u64 = 1692547200000;

fn create_bench_engine_options() -> EngineOptions {
    EngineOptions {
        key_max_size: BENCH_KEY_MAX_SIZE,
        value_max_size: BENCH_VALUE_MAX_SIZE,
        ..Default::default()
    }
}

fn create_data_reader_options() -> ReaderOptions {
    ReaderOptions {
        header_size: 20, // CRC:4 + key_size:4 + value_size:4 + timestamp:8
        key_size_range: ByteRange { start: 4, end: 8 },
        value_size_range: ByteRange { start: 8, end: 12 },
        timestamp_range: ByteRange { start: 12, end: 20 },
        crc_range: ByteRange { start: 0, end: 4 },
    }
}

fn create_hint_reader_options() -> ReaderOptions {
    ReaderOptions::default()
}

fn calculate_crc(data: &[u8]) -> u32 {
    checksum(Crc32IsoHdlc, data) as u32
}

fn create_test_file(data: &[u8]) -> (TempDir, PathBuf) {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let file_path = temp_dir.path().join("bench_file");
    let mut file = File::create(&file_path).expect("Failed to create test file");
    file.write_all(data).expect("Failed to write test data");
    (temp_dir, file_path)
}

fn create_data_entry(key: &[u8], value: &[u8], timestamp: u64) -> Vec<u8> {
    let mut entry = Vec::new();

    // Reserve space for CRC
    entry.extend_from_slice(&[0u8; 4]);
    entry.extend_from_slice(&(key.len() as u32).to_le_bytes());
    entry.extend_from_slice(&(value.len() as u32).to_le_bytes());
    entry.extend_from_slice(&timestamp.to_le_bytes());
    entry.extend_from_slice(key);
    entry.extend_from_slice(value);

    // Calculate and insert CRC
    let payload = &entry[4..];
    let crc = calculate_crc(payload);
    entry[0..4].copy_from_slice(&crc.to_le_bytes());

    entry
}

fn create_hint_entry(key: &[u8], value_size: u32, timestamp: u64, value_offset: u64) -> Vec<u8> {
    let mut entry = Vec::new();
    entry.extend_from_slice(&(key.len() as u32).to_le_bytes());
    entry.extend_from_slice(&value_size.to_le_bytes());
    entry.extend_from_slice(&timestamp.to_le_bytes());
    entry.extend_from_slice(&value_offset.to_le_bytes());
    entry.extend_from_slice(key);
    entry
}

fn create_multi_entry_file(
    entry_count: usize,
    key_size: usize,
    value_size: usize,
    entry_type: &str,
) -> (TempDir, PathBuf, Vec<usize>) {
    let mut file_data = Vec::new();
    let mut offsets = Vec::new();

    for i in 0..entry_count {
        offsets.push(file_data.len());

        let key = format!("key_{i:06}");
        let key_bytes = key.as_bytes();
        let padded_key = if key_bytes.len() < key_size {
            let mut padded = key_bytes.to_vec();
            padded.resize(key_size, b'x');
            padded
        } else {
            key_bytes[..key_size].to_vec()
        };

        let entry_data = match entry_type {
            "data" => {
                let value = vec![b'v'; value_size];
                create_data_entry(&padded_key, &value, BENCH_TIMESTAMP)
            }
            "hint" => create_hint_entry(
                &padded_key,
                value_size as u32,
                BENCH_TIMESTAMP,
                i as u64 * 1000,
            ),
            _ => panic!("Unknown entry type"),
        };

        file_data.extend_from_slice(&entry_data);
    }

    let (temp_dir, file_path) = create_test_file(&file_data);
    (temp_dir, file_path, offsets)
}

// Benchmark single entry parsing
fn bench_single_entry_parsing(c: &mut Criterion) {
    let mut group = c.benchmark_group("single_entry_parsing");

    let sizes = vec![
        (16, 64),     // Small
        (64, 256),    // Medium
        (256, 1024),  // Large
        (1024, 4096), // XL
    ];

    for (key_size, value_size) in sizes {
        let key = vec![b'k'; key_size];
        let value = vec![b'v'; value_size];
        let entry_data = create_data_entry(&key, &value, BENCH_TIMESTAMP);
        let (_temp_dir, file_path) = create_test_file(&entry_data);

        // Benchmark with CRC verification
        let reader_crc = FileReader::<true>::open(
            &file_path,
            BENCH_FILE_ID,
            create_bench_engine_options(),
            create_data_reader_options(),
        )
        .unwrap();

        group.throughput(Throughput::Bytes((key_size + value_size) as u64));
        group.bench_with_input(
            BenchmarkId::new("with_crc", format!("{key_size}k_{value_size}v")),
            &(&reader_crc, 0),
            |b, (reader, offset)| b.iter(|| black_box(reader.parse_entry_ref_at(*offset)).unwrap()),
        );

        // Benchmark without CRC verification
        let reader_no_crc = FileReader::<false>::open(
            &file_path,
            BENCH_FILE_ID,
            create_bench_engine_options(),
            create_data_reader_options(),
        )
        .unwrap();

        group.bench_with_input(
            BenchmarkId::new("no_crc", format!("{key_size}k_{value_size}v")),
            &(&reader_no_crc, 0),
            |b, (reader, offset)| b.iter(|| black_box(reader.parse_entry_ref_at(*offset)).unwrap()),
        );
    }

    group.finish();
}

// Benchmark multiple entry parsing
fn bench_multi_entry_parsing(c: &mut Criterion) {
    let mut group = c.benchmark_group("multi_entry_parsing");

    let configs = vec![
        (100, 32, 128),  // Many small entries
        (50, 128, 512),  // Medium entries
        (10, 512, 2048), // Few large entries
    ];

    for (entry_count, key_size, value_size) in configs {
        let (_temp_dir, file_path, offsets) =
            create_multi_entry_file(entry_count, key_size, value_size, "data");

        let reader = FileReader::<true>::open(
            &file_path,
            BENCH_FILE_ID,
            create_bench_engine_options(),
            create_data_reader_options(),
        )
        .unwrap();

        let total_size = (entry_count * (key_size + value_size)) as u64;
        group.throughput(Throughput::Bytes(total_size));

        group.bench_with_input(
            BenchmarkId::new(
                "sequential_parse",
                format!("{entry_count}entries_{key_size}k_{value_size}v"),
            ),
            &(&reader, &offsets),
            |b, (reader, offsets)| {
                b.iter(|| {
                    for &offset in offsets.iter() {
                        black_box(reader.parse_entry_ref_at(offset)).unwrap();
                    }
                })
            },
        );
    }

    group.finish();
}

// Benchmark random access patterns
fn bench_random_access(c: &mut Criterion) {
    let mut group = c.benchmark_group("random_access");

    let entry_count = 1000;
    let key_size = 64;
    let value_size = 256;

    let (_temp_dir, file_path, offsets) =
        create_multi_entry_file(entry_count, key_size, value_size, "data");

    let reader = FileReader::<true>::open(
        &file_path,
        BENCH_FILE_ID,
        create_bench_engine_options(),
        create_data_reader_options(),
    )
    .unwrap();

    // Create a random access pattern
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher as StdHasher};

    let mut random_indices = Vec::new();
    for i in 0..100 {
        let mut hasher = DefaultHasher::new();
        i.hash(&mut hasher);
        let idx = (hasher.finish() % entry_count as u64) as usize;
        random_indices.push(idx);
    }

    let total_size = (100 * (key_size + value_size)) as u64;
    group.throughput(Throughput::Bytes(total_size));

    group.bench_function("random_100_accesses", |b| {
        b.iter(|| {
            for &idx in &random_indices {
                let offset = offsets[idx];
                black_box(reader.parse_entry_ref_at(offset)).unwrap();
            }
        })
    });

    group.finish();
}

// Benchmark CRC computation overhead
fn bench_crc_overhead(c: &mut Criterion) {
    let mut group = c.benchmark_group("crc_overhead");

    let value_sizes = vec![128, 1024, 8192, 32768]; // Different payload sizes

    for value_size in value_sizes {
        let key = vec![b'k'; 64];
        let value = vec![b'v'; value_size];
        let entry_data = create_data_entry(&key, &value, BENCH_TIMESTAMP);
        let (_temp_dir, file_path) = create_test_file(&entry_data);

        let reader_crc = FileReader::<true>::open(
            &file_path,
            BENCH_FILE_ID,
            create_bench_engine_options(),
            create_data_reader_options(),
        )
        .unwrap();

        let reader_no_crc = FileReader::<false>::open(
            &file_path,
            BENCH_FILE_ID,
            create_bench_engine_options(),
            create_data_reader_options(),
        )
        .unwrap();

        group.throughput(Throughput::Bytes(value_size as u64));

        group.bench_with_input(
            BenchmarkId::new("with_crc", value_size),
            &value_size,
            |b, _| b.iter(|| black_box(reader_crc.parse_entry_ref_at(0)).unwrap()),
        );

        group.bench_with_input(
            BenchmarkId::new("without_crc", value_size),
            &value_size,
            |b, _| b.iter(|| black_box(reader_no_crc.parse_entry_ref_at(0)).unwrap()),
        );
    }

    group.finish();
}

// Benchmark hint file parsing
fn bench_hint_parsing(c: &mut Criterion) {
    let mut group = c.benchmark_group("hint_parsing");

    let configs = vec![
        (1000, 32), // Many small keys
        (500, 128), // Medium keys
        (100, 512), // Large keys
    ];

    for (entry_count, key_size) in configs {
        let (_temp_dir, file_path, offsets) =
            create_multi_entry_file(entry_count, key_size, 256, "hint");

        let reader = FileReader::<false>::open(
            &file_path,
            BENCH_FILE_ID,
            create_bench_engine_options(),
            create_hint_reader_options(),
        )
        .unwrap();

        let total_size = (entry_count * key_size) as u64;
        group.throughput(Throughput::Bytes(total_size));

        group.bench_with_input(
            BenchmarkId::new(
                "hint_sequential",
                format!("{entry_count}entries_{key_size}k"),
            ),
            &(&reader, &offsets),
            |b, (reader, offsets)| {
                b.iter(|| {
                    for &offset in offsets.iter() {
                        black_box(reader.parse_entry_ref_at(offset)).unwrap();
                    }
                })
            },
        );
    }

    group.finish();
}

// Benchmark memory access patterns
fn bench_memory_access(c: &mut Criterion) {
    let mut group = c.benchmark_group("memory_access");

    let entry_count = 1000;
    let key_size = 64;
    let value_size = 1024;

    let (_temp_dir, file_path, _) =
        create_multi_entry_file(entry_count, key_size, value_size, "data");

    let reader = FileReader::<false>::open(
        &file_path,
        BENCH_FILE_ID,
        create_bench_engine_options(),
        create_data_reader_options(),
    )
    .unwrap();

    // Test different read sizes
    let read_sizes = vec![16, 64, 256, 1024, 4096];

    for read_size in read_sizes {
        group.throughput(Throughput::Bytes(read_size as u64));

        group.bench_with_input(
            BenchmarkId::new("read_at", read_size),
            &read_size,
            |b, &size| b.iter(|| black_box(reader.read_at(0, size)).unwrap()),
        );
    }

    group.finish();
}

// Benchmark entry conversion (EntryRef to Entry)
fn bench_entry_conversion(c: &mut Criterion) {
    let mut group = c.benchmark_group("entry_conversion");

    let sizes = vec![(32, 128), (128, 512), (512, 2048)];

    for (key_size, value_size) in sizes {
        let key = vec![b'k'; key_size];
        let value = vec![b'v'; value_size];
        let entry_data = create_data_entry(&key, &value, BENCH_TIMESTAMP);
        let (_temp_dir, file_path) = create_test_file(&entry_data);

        let reader = FileReader::<false>::open(
            &file_path,
            BENCH_FILE_ID,
            create_bench_engine_options(),
            create_data_reader_options(),
        )
        .unwrap();

        group.throughput(Throughput::Bytes((key_size + value_size) as u64));

        group.bench_with_input(
            BenchmarkId::new("entry_ref", format!("{key_size}k_{value_size}v")),
            &key_size,
            |b, _| b.iter(|| black_box(reader.parse_entry_ref_at(0)).unwrap()),
        );

        group.bench_with_input(
            BenchmarkId::new("entry_owned", format!("{key_size}k_{value_size}v")),
            &key_size,
            |b, _| b.iter(|| black_box(reader.parse_entry_at(0)).unwrap()),
        );
    }

    group.finish();
}

// Benchmark concurrent access from multiple threads
fn bench_concurrent_access(c: &mut Criterion) {
    let mut group = c.benchmark_group("concurrent_access");

    // Create a file with multiple entries for different threads to access
    let entry_count = 1000;
    let key_size = 64;
    let value_size = 256;

    let (_temp_dir, file_path, offsets) =
        create_multi_entry_file(entry_count, key_size, value_size, "data");

    // Test different thread counts
    let thread_counts = vec![1, 2, 4, 8];
    let operations_per_thread = 100;

    for thread_count in thread_counts {
        // Test without CRC (faster, shows thread scaling better)
        let reader = std::sync::Arc::new(
            FileReader::<false>::open(
                &file_path,
                BENCH_FILE_ID,
                create_bench_engine_options(),
                create_data_reader_options(),
            )
            .unwrap(),
        );

        let total_operations = thread_count * operations_per_thread;
        let total_bytes = (total_operations * (key_size + value_size)) as u64;
        group.throughput(Throughput::Bytes(total_bytes));

        group.bench_with_input(
            BenchmarkId::new("no_crc", format!("{thread_count}threads")),
            &thread_count,
            |b, &num_threads| {
                b.iter(|| {
                    let handles: Vec<_> = (0..num_threads)
                        .map(|thread_id| {
                            let reader_clone = reader.clone();
                            let offsets_clone = offsets.clone();

                            std::thread::spawn(move || {
                                // Each thread accesses different entries to avoid cache effects
                                let start_idx = (thread_id * operations_per_thread) % entry_count;

                                for i in 0..operations_per_thread {
                                    let idx = (start_idx + i) % entry_count;
                                    let offset = offsets_clone[idx];
                                    black_box(reader_clone.parse_entry_ref_at(offset)).unwrap();
                                }
                            })
                        })
                        .collect();

                    for handle in handles {
                        handle.join().unwrap();
                    }
                })
            },
        );

        // Test with CRC (shows overhead under contention)
        let reader_crc = std::sync::Arc::new(
            FileReader::<true>::open(
                &file_path,
                BENCH_FILE_ID,
                create_bench_engine_options(),
                create_data_reader_options(),
            )
            .unwrap(),
        );

        group.bench_with_input(
            BenchmarkId::new("with_crc", format!("{thread_count}threads")),
            &thread_count,
            |b, &num_threads| {
                b.iter(|| {
                    let handles: Vec<_> = (0..num_threads)
                        .map(|thread_id| {
                            let reader_clone = reader_crc.clone();
                            let offsets_clone = offsets.clone();

                            std::thread::spawn(move || {
                                let start_idx = (thread_id * operations_per_thread) % entry_count;

                                for i in 0..operations_per_thread {
                                    let idx = (start_idx + i) % entry_count;
                                    let offset = offsets_clone[idx];
                                    black_box(reader_clone.parse_entry_ref_at(offset)).unwrap();
                                }
                            })
                        })
                        .collect();

                    for handle in handles {
                        handle.join().unwrap();
                    }
                })
            },
        );
    }

    group.finish();
}

// Benchmark concurrent access patterns (same vs different entries)
fn bench_concurrent_patterns(c: &mut Criterion) {
    let mut group = c.benchmark_group("concurrent_patterns");

    let entry_count = 100;
    let key_size = 64;
    let value_size = 256;
    let thread_count = 4;
    let operations_per_thread = 50;

    let (_temp_dir, file_path, offsets) =
        create_multi_entry_file(entry_count, key_size, value_size, "data");

    let reader = std::sync::Arc::new(
        FileReader::<false>::open(
            &file_path,
            BENCH_FILE_ID,
            create_bench_engine_options(),
            create_data_reader_options(),
        )
        .unwrap(),
    );

    let total_bytes = (thread_count * operations_per_thread * (key_size + value_size)) as u64;
    group.throughput(Throughput::Bytes(total_bytes));

    // Pattern 1: All threads access the same entry (worst case for cache)
    group.bench_function("same_entry", |b| {
        b.iter(|| {
            let handles: Vec<_> = (0..thread_count)
                .map(|_| {
                    let reader_clone = reader.clone();
                    let same_offset = offsets[0]; // All threads use same offset

                    std::thread::spawn(move || {
                        for _ in 0..operations_per_thread {
                            black_box(reader_clone.parse_entry_ref_at(same_offset)).unwrap();
                        }
                    })
                })
                .collect();

            for handle in handles {
                handle.join().unwrap();
            }
        })
    });

    // Pattern 2: Each thread accesses different entries (best case)
    group.bench_function("different_entries", |b| {
        b.iter(|| {
            let handles: Vec<_> = (0..thread_count)
                .map(|thread_id| {
                    let reader_clone = reader.clone();
                    let offsets_clone = offsets.clone();

                    std::thread::spawn(move || {
                        let start_idx = thread_id * operations_per_thread;

                        for i in 0..operations_per_thread {
                            let idx = (start_idx + i) % entry_count;
                            let offset = offsets_clone[idx];
                            black_box(reader_clone.parse_entry_ref_at(offset)).unwrap();
                        }
                    })
                })
                .collect();

            for handle in handles {
                handle.join().unwrap();
            }
        })
    });

    // Pattern 3: Threads access overlapping but not identical entries
    group.bench_function("overlapping_entries", |b| {
        b.iter(|| {
            let handles: Vec<_> = (0..thread_count)
                .map(|thread_id| {
                    let reader_clone = reader.clone();
                    let offsets_clone = offsets.clone();

                    std::thread::spawn(move || {
                        // Each thread accesses a sliding window of entries
                        let window_start = (thread_id * operations_per_thread / 2) % entry_count;

                        for i in 0..operations_per_thread {
                            let idx = (window_start + i) % entry_count;
                            let offset = offsets_clone[idx];
                            black_box(reader_clone.parse_entry_ref_at(offset)).unwrap();
                        }
                    })
                })
                .collect();

            for handle in handles {
                handle.join().unwrap();
            }
        })
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_single_entry_parsing,
    bench_multi_entry_parsing,
    bench_random_access,
    bench_crc_overhead,
    bench_hint_parsing,
    bench_memory_access,
    bench_entry_conversion,
    bench_concurrent_access,
    bench_concurrent_patterns,
);

criterion_main!(benches);
