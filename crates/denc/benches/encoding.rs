use bytes::BytesMut;
use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};
use denc::Denc;

fn bench_u32_encode(c: &mut Criterion) {
    let mut group = c.benchmark_group("u32_encode");
    group.throughput(Throughput::Bytes(4));

    group.bench_function("encode", |b| {
        let value: u32 = 0x12345678;
        let mut buf = BytesMut::with_capacity(4);

        b.iter(|| {
            buf.clear();
            value.encode(&mut buf, 0).unwrap();
            black_box(());
        });
    });

    group.finish();
}

fn bench_u32_decode(c: &mut Criterion) {
    let mut group = c.benchmark_group("u32_decode");
    group.throughput(Throughput::Bytes(4));

    group.bench_function("decode", |b| {
        let value: u32 = 0x12345678;
        let mut buf = BytesMut::with_capacity(4);
        value.encode(&mut buf, 0).unwrap();

        b.iter(|| {
            let mut buf_copy = buf.clone();
            black_box(u32::decode(&mut buf_copy, 0).unwrap());
        });
    });

    group.finish();
}

fn bench_u64_encode(c: &mut Criterion) {
    let mut group = c.benchmark_group("u64_encode");
    group.throughput(Throughput::Bytes(8));

    group.bench_function("encode", |b| {
        let value: u64 = 0x123456789ABCDEF0;
        let mut buf = BytesMut::with_capacity(8);

        b.iter(|| {
            buf.clear();
            value.encode(&mut buf, 0).unwrap();
            black_box(());
        });
    });

    group.finish();
}

fn bench_u64_decode(c: &mut Criterion) {
    let mut group = c.benchmark_group("u64_decode");
    group.throughput(Throughput::Bytes(8));

    group.bench_function("decode", |b| {
        let value: u64 = 0x123456789ABCDEF0;
        let mut buf = BytesMut::with_capacity(8);
        value.encode(&mut buf, 0).unwrap();

        b.iter(|| {
            let mut buf_copy = buf.clone();
            black_box(u64::decode(&mut buf_copy, 0).unwrap());
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_u32_encode,
    bench_u32_decode,
    bench_u64_encode,
    bench_u64_decode
);
criterion_main!(benches);
