use bytes::Bytes;
use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use msgr2::ceph_message::CephMessagePayload;
use osdclient::messages::MOSDOp;
use osdclient::types::{OSDOp, ObjectId, RequestId, StripedPgId};

fn bench_osdop_read(c: &mut Criterion) {
    let mut group = c.benchmark_group("osdop_build");

    for size in [4096, 65536, 1048576] {
        group.bench_with_input(BenchmarkId::new("read", size), &size, |b, &size| {
            b.iter(|| {
                let op = OSDOp::read(black_box(0), black_box(size));
                black_box(op);
            });
        });
    }

    group.finish();
}

fn bench_osdop_write(c: &mut Criterion) {
    let mut group = c.benchmark_group("osdop_build");

    for size in [4096, 65536, 1048576] {
        let data = Bytes::from(vec![0u8; size as usize]);
        group.bench_with_input(BenchmarkId::new("write", size), &size, |b, &_size| {
            b.iter(|| {
                let op = OSDOp::write(black_box(0), black_box(data.clone()));
                black_box(op);
            });
        });
    }

    group.finish();
}

fn bench_osdop_stat(c: &mut Criterion) {
    let mut group = c.benchmark_group("osdop_build");

    group.bench_function("stat", |b| {
        b.iter(|| {
            let op = OSDOp::stat();
            black_box(op);
        });
    });

    group.finish();
}

fn bench_mosdop_encode_read(c: &mut Criterion) {
    let mut group = c.benchmark_group("mosdop_encode");

    for size in [4096, 65536, 1048576] {
        let object = ObjectId::new(1, "test_object");
        let pgid = StripedPgId::from_pg(1, 0x12345678);
        let ops = vec![OSDOp::read(0, size)];
        let reqid = RequestId::new("client.0", 1, 1);
        let mosdop = MOSDOp::new(
            1,
            1,
            MOSDOp::calculate_flags(&ops),
            object,
            pgid,
            ops,
            reqid,
            0,
        );

        group.throughput(Throughput::Bytes(size));
        group.bench_with_input(BenchmarkId::new("read", size), &size, |b, &_size| {
            b.iter(|| {
                let encoded = black_box(&mosdop).encode_payload(black_box(0)).unwrap();
                black_box(encoded);
            });
        });
    }

    group.finish();
}

fn bench_mosdop_encode_write(c: &mut Criterion) {
    let mut group = c.benchmark_group("mosdop_encode");

    for size in [4096, 65536, 1048576] {
        let data = Bytes::from(vec![0u8; size as usize]);
        let object = ObjectId::new(1, "test_object");
        let pgid = StripedPgId::from_pg(1, 0x12345678);
        let ops = vec![OSDOp::write(0, data)];
        let reqid = RequestId::new("client.0", 1, 1);
        let mosdop = MOSDOp::new(
            1,
            1,
            MOSDOp::calculate_flags(&ops),
            object,
            pgid,
            ops,
            reqid,
            0,
        );

        group.throughput(Throughput::Bytes(size));
        group.bench_with_input(BenchmarkId::new("write", size), &size, |b, &_size| {
            b.iter(|| {
                let encoded = black_box(&mosdop).encode_payload(black_box(0)).unwrap();
                black_box(encoded);
            });
        });
    }

    group.finish();
}

fn bench_mosdop_encode_stat(c: &mut Criterion) {
    let mut group = c.benchmark_group("mosdop_encode");

    let object = ObjectId::new(1, "test_object");
    let pgid = StripedPgId::from_pg(1, 0x12345678);
    let ops = vec![OSDOp::stat()];
    let reqid = RequestId::new("client.0", 1, 1);
    let mosdop = MOSDOp::new(
        1,
        1,
        MOSDOp::calculate_flags(&ops),
        object,
        pgid,
        ops,
        reqid,
        0,
    );

    group.bench_function("stat", |b| {
        b.iter(|| {
            let encoded = black_box(&mosdop).encode_payload(black_box(0)).unwrap();
            black_box(encoded);
        });
    });

    group.finish();
}

fn bench_mosdop_full_message_encode(c: &mut Criterion) {
    let mut group = c.benchmark_group("mosdop_full_message");

    for size in [4096, 65536, 1048576] {
        let data = Bytes::from(vec![0u8; size as usize]);
        let object = ObjectId::new(1, "test_object");
        let pgid = StripedPgId::from_pg(1, 0x12345678);
        let ops = vec![OSDOp::write(0, data)];
        let reqid = RequestId::new("client.0", 1, 1);
        let mosdop = MOSDOp::new(
            1,
            1,
            MOSDOp::calculate_flags(&ops),
            object,
            pgid,
            ops,
            reqid,
            0,
        );

        group.throughput(Throughput::Bytes(size));
        group.bench_with_input(BenchmarkId::new("write", size), &size, |b, &_size| {
            b.iter(|| {
                use msgr2::ceph_message::{CephMessage, CrcFlags};
                let msg =
                    CephMessage::from_payload(black_box(&mosdop), black_box(0), CrcFlags::ALL)
                        .unwrap();
                black_box(msg);
            });
        });
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_osdop_read,
    bench_osdop_write,
    bench_osdop_stat,
    bench_mosdop_encode_read,
    bench_mosdop_encode_write,
    bench_mosdop_encode_stat,
    bench_mosdop_full_message_encode,
);
criterion_main!(benches);
