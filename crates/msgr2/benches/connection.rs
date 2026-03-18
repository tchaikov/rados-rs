use bytes::Bytes;
use criterion::{Criterion, Throughput, black_box, criterion_group, criterion_main};
use msgr2::frames::{
    AuthDoneFrame, AuthRequestFrame, ClientIdentFrame, FrameAssembler, HelloFrame, MessageFrame,
};
use msgr2::header::MsgHeader;

fn bench_hello_frame_encode(c: &mut Criterion) {
    let mut group = c.benchmark_group("hello_frame");

    let addr = std::net::SocketAddr::from(([127, 0, 0, 1], 6789));
    let entity_addr = denc::EntityAddr::from_socket_addr(denc::EntityAddrType::Msgr2, addr);
    let frame = HelloFrame::new(1, entity_addr);
    let mut assembler = FrameAssembler::new(true);
    let features = 0;

    group.bench_function("encode", |b| {
        b.iter(|| {
            let wire = assembler
                .to_wire(black_box(&frame), black_box(features))
                .unwrap();
            black_box(wire);
        });
    });

    group.finish();
}

fn bench_hello_frame_decode(c: &mut Criterion) {
    let mut group = c.benchmark_group("hello_frame");

    let addr = std::net::SocketAddr::from(([127, 0, 0, 1], 6789));
    let entity_addr = denc::EntityAddr::from_socket_addr(denc::EntityAddrType::Msgr2, addr);
    let frame = HelloFrame::new(1, entity_addr);
    let mut assembler = FrameAssembler::new(true);
    let wire = assembler.to_wire(&frame, 0).unwrap();

    group.bench_function("decode", |b| {
        b.iter(|| {
            let decoded = HelloFrame::from_wire(black_box(wire.clone())).unwrap();
            black_box(decoded);
        });
    });

    group.finish();
}

fn bench_auth_request_frame_encode(c: &mut Criterion) {
    let mut group = c.benchmark_group("auth_request_frame");

    let frame = AuthRequestFrame::new(
        2,                           // CEPHX
        vec![1, 2],                  // preferred modes
        Bytes::from(vec![0u8; 256]), // 256-byte auth payload
    );
    let mut assembler = FrameAssembler::new(true);
    let features = 0;

    group.bench_function("encode", |b| {
        b.iter(|| {
            let wire = assembler
                .to_wire(black_box(&frame), black_box(features))
                .unwrap();
            black_box(wire);
        });
    });

    group.finish();
}

fn bench_auth_request_frame_decode(c: &mut Criterion) {
    let mut group = c.benchmark_group("auth_request_frame");

    let frame = AuthRequestFrame::new(2, vec![1, 2], Bytes::from(vec![0u8; 256]));
    let mut assembler = FrameAssembler::new(true);
    let wire = assembler.to_wire(&frame, 0).unwrap();

    group.bench_function("decode", |b| {
        b.iter(|| {
            let decoded = AuthRequestFrame::from_wire(black_box(wire.clone())).unwrap();
            black_box(decoded);
        });
    });

    group.finish();
}

fn bench_auth_done_frame_encode(c: &mut Criterion) {
    let mut group = c.benchmark_group("auth_done_frame");

    let frame = AuthDoneFrame::new(
        12345,                       // global_id
        1,                           // con_mode
        Bytes::from(vec![0u8; 128]), // 128-byte auth payload
    );
    let mut assembler = FrameAssembler::new(true);
    let features = 0;

    group.bench_function("encode", |b| {
        b.iter(|| {
            let wire = assembler
                .to_wire(black_box(&frame), black_box(features))
                .unwrap();
            black_box(wire);
        });
    });

    group.finish();
}

fn bench_auth_done_frame_decode(c: &mut Criterion) {
    let mut group = c.benchmark_group("auth_done_frame");

    let frame = AuthDoneFrame::new(12345, 1, Bytes::from(vec![0u8; 128]));
    let mut assembler = FrameAssembler::new(true);
    let wire = assembler.to_wire(&frame, 0).unwrap();

    group.bench_function("decode", |b| {
        b.iter(|| {
            let decoded = AuthDoneFrame::from_wire(black_box(wire.clone())).unwrap();
            black_box(decoded);
        });
    });

    group.finish();
}

fn bench_client_ident_frame_encode(c: &mut Criterion) {
    let mut group = c.benchmark_group("client_ident_frame");

    let addr = std::net::SocketAddr::from(([127, 0, 0, 1], 6789));
    let entity_addr = denc::EntityAddr::from_socket_addr(denc::EntityAddrType::Msgr2, addr);
    let target_addr = denc::EntityAddr::from_socket_addr(denc::EntityAddrType::Msgr2, addr);
    let frame = ClientIdentFrame::new(
        denc::EntityAddrvec::with_addr(entity_addr.clone()),
        target_addr,
        12345,              // gid
        100,                // global_seq
        0xFFFFFFFFFFFFFFFF, // features_supported
        0,                  // features_required
        0,                  // flags
        0xABCDEF,           // cookie
    );
    let mut assembler = FrameAssembler::new(true);
    let features = denc::CephFeatures::MSG_ADDR2.bits();

    group.bench_function("encode", |b| {
        b.iter(|| {
            let wire = assembler
                .to_wire(black_box(&frame), black_box(features))
                .unwrap();
            black_box(wire);
        });
    });

    group.finish();
}

fn bench_client_ident_frame_decode(c: &mut Criterion) {
    let mut group = c.benchmark_group("client_ident_frame");

    let addr = std::net::SocketAddr::from(([127, 0, 0, 1], 6789));
    let entity_addr = denc::EntityAddr::from_socket_addr(denc::EntityAddrType::Msgr2, addr);
    let target_addr = denc::EntityAddr::from_socket_addr(denc::EntityAddrType::Msgr2, addr);
    let frame = ClientIdentFrame::new(
        denc::EntityAddrvec::with_addr(entity_addr),
        target_addr,
        12345,
        100,
        0xFFFFFFFFFFFFFFFF,
        0,
        0,
        0xABCDEF,
    );
    let mut assembler = FrameAssembler::new(true);
    let features = denc::CephFeatures::MSG_ADDR2.bits();
    let wire = assembler.to_wire(&frame, features).unwrap();

    group.bench_function("decode", |b| {
        b.iter(|| {
            let decoded = ClientIdentFrame::from_wire(black_box(wire.clone())).unwrap();
            black_box(decoded);
        });
    });

    group.finish();
}

fn bench_message_frame_encode(c: &mut Criterion) {
    let mut group = c.benchmark_group("message_frame");

    // Small message (1KB front, no middle/data)
    let header = MsgHeader::new_default(1, 100);
    let front = Bytes::from(vec![0u8; 1024]);
    let frame = MessageFrame::new(header, front, Bytes::new(), Bytes::new());
    let mut assembler = FrameAssembler::new(true);
    let features = 0;

    group.throughput(Throughput::Bytes(1024));
    group.bench_function("encode_1kb", |b| {
        b.iter(|| {
            let wire = assembler
                .to_wire(black_box(&frame), black_box(features))
                .unwrap();
            black_box(wire);
        });
    });

    group.finish();
}

fn bench_message_frame_decode(c: &mut Criterion) {
    let mut group = c.benchmark_group("message_frame");

    let header = MsgHeader::new_default(1, 100);
    let front = Bytes::from(vec![0u8; 1024]);
    let frame = MessageFrame::new(header, front, Bytes::new(), Bytes::new());
    let mut assembler = FrameAssembler::new(true);
    let wire = assembler.to_wire(&frame, 0).unwrap();

    group.throughput(Throughput::Bytes(1024));
    group.bench_function("decode_1kb", |b| {
        b.iter(|| {
            let decoded = MessageFrame::from_wire(black_box(wire.clone())).unwrap();
            black_box(decoded);
        });
    });

    group.finish();
}

fn bench_message_frame_large(c: &mut Criterion) {
    let mut group = c.benchmark_group("message_frame_large");

    // Large message (4KB front, 4KB middle, 64KB data)
    let header = MsgHeader::new_default(1, 100);
    let front = Bytes::from(vec![0u8; 4096]);
    let middle = Bytes::from(vec![0u8; 4096]);
    let data = Bytes::from(vec![0u8; 65536]);
    let frame = MessageFrame::new(header, front, middle, data);
    let mut assembler = FrameAssembler::new(true);
    let features = 0;

    let total_size = 4096 + 4096 + 65536;
    group.throughput(Throughput::Bytes(total_size));

    group.bench_function("encode_72kb", |b| {
        b.iter(|| {
            let wire = assembler
                .to_wire(black_box(&frame), black_box(features))
                .unwrap();
            black_box(wire);
        });
    });

    let wire = assembler.to_wire(&frame, 0).unwrap();
    group.bench_function("decode_72kb", |b| {
        b.iter(|| {
            let decoded = MessageFrame::from_wire(black_box(wire.clone())).unwrap();
            black_box(decoded);
        });
    });

    group.finish();
}

fn bench_frame_assembly_rev0_vs_rev1(c: &mut Criterion) {
    let mut group = c.benchmark_group("frame_assembly");

    let addr = std::net::SocketAddr::from(([127, 0, 0, 1], 6789));
    let entity_addr = denc::EntityAddr::from_socket_addr(denc::EntityAddrType::Msgr2, addr);
    let frame = HelloFrame::new(1, entity_addr);
    let features = 0;

    group.bench_function("rev0", |b| {
        let mut assembler = FrameAssembler::new(false);
        b.iter(|| {
            let wire = assembler
                .to_wire(black_box(&frame), black_box(features))
                .unwrap();
            black_box(wire);
        });
    });

    group.bench_function("rev1", |b| {
        let mut assembler = FrameAssembler::new(true);
        b.iter(|| {
            let wire = assembler
                .to_wire(black_box(&frame), black_box(features))
                .unwrap();
            black_box(wire);
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_hello_frame_encode,
    bench_hello_frame_decode,
    bench_auth_request_frame_encode,
    bench_auth_request_frame_decode,
    bench_auth_done_frame_encode,
    bench_auth_done_frame_decode,
    bench_client_ident_frame_encode,
    bench_client_ident_frame_decode,
    bench_message_frame_encode,
    bench_message_frame_decode,
    bench_message_frame_large,
    bench_frame_assembly_rev0_vs_rev1,
);
criterion_main!(benches);
