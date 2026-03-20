use bytes::{Bytes, BytesMut};
use criterion::{Criterion, Throughput, black_box, criterion_group, criterion_main};
use rados::denc::{Denc, EntityAddr, EntityAddrType, EntityName, UTime};

// ============= Primitive Type Benchmarks =============

fn bench_u32_encode(c: &mut Criterion) {
    let mut group = c.benchmark_group("primitives/u32");
    group.throughput(Throughput::Bytes(4));

    group.bench_function("encode", |b| {
        let value: u32 = 0x12345678;
        let mut buf = BytesMut::with_capacity(4);

        b.iter(|| {
            buf.clear();
            value.encode(&mut buf, 0).unwrap();
            black_box(&buf);
        });
    });

    group.bench_function("decode", |b| {
        let value: u32 = 0x12345678;
        let mut buf = BytesMut::with_capacity(4);
        value.encode(&mut buf, 0).unwrap();
        let encoded = buf.freeze();

        b.iter(|| {
            let mut data = encoded.clone();
            black_box(u32::decode(&mut data, 0).unwrap());
        });
    });

    group.finish();
}

fn bench_string_encode(c: &mut Criterion) {
    let mut group = c.benchmark_group("primitives/string");

    // Test with different string sizes
    for size in [10, 100, 1000].iter() {
        let s = "x".repeat(*size);
        group.throughput(Throughput::Bytes((4 + size) as u64)); // length prefix + data

        group.bench_function(format!("encode/{}", size), |b| {
            let mut buf = BytesMut::with_capacity(4 + size);

            b.iter(|| {
                buf.clear();
                s.encode(&mut buf, 0).unwrap();
                black_box(&buf);
            });
        });

        group.bench_function(format!("decode/{}", size), |b| {
            let mut buf = BytesMut::with_capacity(4 + size);
            s.encode(&mut buf, 0).unwrap();
            let encoded = buf.freeze();

            b.iter(|| {
                let mut data = encoded.clone();
                black_box(String::decode(&mut data, 0).unwrap());
            });
        });
    }

    group.finish();
}

fn bench_bytes_encode(c: &mut Criterion) {
    let mut group = c.benchmark_group("primitives/bytes");

    // Test with different byte sizes
    for size in [64, 512, 4096].iter() {
        let data = vec![0xAB; *size];
        let bytes = Bytes::from(data);
        group.throughput(Throughput::Bytes((4 + size) as u64)); // length prefix + data

        group.bench_function(format!("encode/{}", size), |b| {
            let mut buf = BytesMut::with_capacity(4 + size);

            b.iter(|| {
                buf.clear();
                bytes.encode(&mut buf, 0).unwrap();
                black_box(&buf);
            });
        });

        group.bench_function(format!("decode/{}", size), |b| {
            let mut buf = BytesMut::with_capacity(4 + size);
            bytes.encode(&mut buf, 0).unwrap();
            let encoded = buf.freeze();

            b.iter(|| {
                let mut data = encoded.clone();
                black_box(Bytes::decode(&mut data, 0).unwrap());
            });
        });
    }

    group.finish();
}

// ============= Complex Type Benchmarks =============

fn bench_utime_encode(c: &mut Criterion) {
    let mut group = c.benchmark_group("complex/utime");
    group.throughput(Throughput::Bytes(8)); // 2 * u32

    let utime = UTime::new(1234567890, 123456789);

    group.bench_function("encode", |b| {
        let mut buf = BytesMut::with_capacity(8);

        b.iter(|| {
            buf.clear();
            utime.encode(&mut buf, 0).unwrap();
            black_box(&buf);
        });
    });

    group.bench_function("decode", |b| {
        let mut buf = BytesMut::with_capacity(8);
        utime.encode(&mut buf, 0).unwrap();
        let encoded = buf.freeze();

        b.iter(|| {
            let mut data = encoded.clone();
            black_box(UTime::decode(&mut data, 0).unwrap());
        });
    });

    group.finish();
}

fn bench_entity_name_encode(c: &mut Criterion) {
    let mut group = c.benchmark_group("complex/entity_name");

    let entity_name = EntityName::client("admin");
    let size = entity_name.encoded_size(0).unwrap();
    group.throughput(Throughput::Bytes(size as u64));

    group.bench_function("encode", |b| {
        let mut buf = BytesMut::with_capacity(size);

        b.iter(|| {
            buf.clear();
            entity_name.encode(&mut buf, 0).unwrap();
            black_box(&buf);
        });
    });

    group.bench_function("decode", |b| {
        let mut buf = BytesMut::with_capacity(size);
        entity_name.encode(&mut buf, 0).unwrap();
        let encoded = buf.freeze();

        b.iter(|| {
            let mut data = encoded.clone();
            black_box(EntityName::decode(&mut data, 0).unwrap());
        });
    });

    group.finish();
}

fn bench_entity_addr_encode(c: &mut Criterion) {
    let mut group = c.benchmark_group("complex/entity_addr");

    // Create a realistic EntityAddr with IPv4
    let addr = std::net::SocketAddr::from(([127, 0, 0, 1], 6789));
    let entity_addr = EntityAddr::from_socket_addr(EntityAddrType::Msgr2, addr);

    let size = entity_addr.encoded_size(0).unwrap();
    group.throughput(Throughput::Bytes(size as u64));

    group.bench_function("encode/modern", |b| {
        let mut buf = BytesMut::with_capacity(size);

        b.iter(|| {
            buf.clear();
            entity_addr.encode(&mut buf, 0).unwrap();
            black_box(&buf);
        });
    });

    group.bench_function("decode/modern", |b| {
        let mut buf = BytesMut::with_capacity(size);
        entity_addr.encode(&mut buf, 0).unwrap();
        let encoded = buf.freeze();

        b.iter(|| {
            let mut data = encoded.clone();
            black_box(EntityAddr::decode(&mut data, 0).unwrap());
        });
    });

    group.finish();
}

// ============= Composite Message Benchmarks =============

fn bench_composite_message(c: &mut Criterion) {
    let mut group = c.benchmark_group("composite/message");

    // Simulate a typical message with multiple fields
    let entity_name = EntityName::client("admin");
    let addr = std::net::SocketAddr::from(([10, 0, 0, 1], 6800));
    let entity_addr = EntityAddr::from_socket_addr(EntityAddrType::Msgr2, addr);
    let timestamp = UTime::new(1234567890, 123456789);
    let payload = Bytes::from(vec![0u8; 256]);

    let total_size = entity_name.encoded_size(0).unwrap()
        + entity_addr
            .encoded_size(rados::denc::CephFeatures::MSG_ADDR2.bits())
            .unwrap()
        + timestamp.encoded_size(0).unwrap()
        + payload.encoded_size(0).unwrap();

    group.throughput(Throughput::Bytes(total_size as u64));

    group.bench_function("encode", |b| {
        let mut buf = BytesMut::with_capacity(total_size);

        b.iter(|| {
            buf.clear();
            entity_name.encode(&mut buf, 0).unwrap();
            entity_addr
                .encode(&mut buf, rados::denc::CephFeatures::MSG_ADDR2.bits())
                .unwrap();
            timestamp.encode(&mut buf, 0).unwrap();
            payload.encode(&mut buf, 0).unwrap();
            black_box(&buf);
        });
    });

    group.bench_function("decode", |b| {
        let mut buf = BytesMut::with_capacity(total_size);
        entity_name.encode(&mut buf, 0).unwrap();
        entity_addr
            .encode(&mut buf, rados::denc::CephFeatures::MSG_ADDR2.bits())
            .unwrap();
        timestamp.encode(&mut buf, 0).unwrap();
        payload.encode(&mut buf, 0).unwrap();
        let encoded = buf.freeze();

        b.iter(|| {
            let mut data = encoded.clone();
            let _name = EntityName::decode(&mut data, 0).unwrap();
            let _addr =
                EntityAddr::decode(&mut data, rados::denc::CephFeatures::MSG_ADDR2.bits()).unwrap();
            let _time = UTime::decode(&mut data, 0).unwrap();
            let _payload = Bytes::decode(&mut data, 0).unwrap();
            black_box(());
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_u32_encode,
    bench_string_encode,
    bench_bytes_encode,
    bench_utime_encode,
    bench_entity_name_encode,
    bench_entity_addr_encode,
    bench_composite_message
);
criterion_main!(benches);
