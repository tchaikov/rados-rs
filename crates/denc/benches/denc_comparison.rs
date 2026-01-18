use bytes::BytesMut;
use criterion::{black_box, criterion_group, criterion_main, Criterion};
use denc::denc::Denc;
use denc::denc_mut::DencMut;
use denc::osdmap::{EVersion, OsdInfo, PgId, UTime, UuidD};

fn bench_pgid_old(c: &mut Criterion) {
    let pgid = PgId {
        pool: 12345,
        seed: 67890,
    };

    c.bench_function("PgId::encode (old Denc)", |b| {
        b.iter(|| {
            let _ = black_box(<PgId as Denc>::encode(&pgid, 0).unwrap());
        });
    });
}

fn bench_pgid_new(c: &mut Criterion) {
    let pgid = PgId {
        pool: 12345,
        seed: 67890,
    };

    c.bench_function("PgId::encode (new DencMut)", |b| {
        b.iter(|| {
            let mut buf = BytesMut::with_capacity(17);
            <PgId as DencMut>::encode(&pgid, &mut buf, 0).unwrap();
            let _ = black_box(buf.freeze());
        });
    });
}

fn bench_eversion_old(c: &mut Criterion) {
    let eversion = EVersion {
        version: 123456789,
        epoch: 42,
        pad: 0,
    };

    c.bench_function("EVersion::encode (old Denc)", |b| {
        b.iter(|| {
            let _ = black_box(<EVersion as Denc>::encode(&eversion, 0).unwrap());
        });
    });
}

fn bench_eversion_new(c: &mut Criterion) {
    let eversion = EVersion {
        version: 123456789,
        epoch: 42,
        pad: 0,
    };

    c.bench_function("EVersion::encode (new DencMut)", |b| {
        b.iter(|| {
            let mut buf = BytesMut::with_capacity(12);
            <EVersion as DencMut>::encode(&eversion, &mut buf, 0).unwrap();
            let _ = black_box(buf.freeze());
        });
    });
}

fn bench_utime_old(c: &mut Criterion) {
    let utime = UTime {
        sec: 1234567890,
        nsec: 123456,
    };

    c.bench_function("UTime::encode (old Denc)", |b| {
        b.iter(|| {
            let _ = black_box(<UTime as Denc>::encode(&utime, 0).unwrap());
        });
    });
}

fn bench_utime_new(c: &mut Criterion) {
    let utime = UTime {
        sec: 1234567890,
        nsec: 123456,
    };

    c.bench_function("UTime::encode (new DencMut)", |b| {
        b.iter(|| {
            let mut buf = BytesMut::with_capacity(8);
            <UTime as DencMut>::encode(&utime, &mut buf, 0).unwrap();
            let _ = black_box(buf.freeze());
        });
    });
}

fn bench_uuidd_old(c: &mut Criterion) {
    let uuid = UuidD::from_bytes([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]);

    c.bench_function("UuidD::encode (old Denc)", |b| {
        b.iter(|| {
            let _ = black_box(<UuidD as Denc>::encode(&uuid, 0).unwrap());
        });
    });
}

fn bench_uuidd_new(c: &mut Criterion) {
    let uuid = UuidD::from_bytes([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]);

    c.bench_function("UuidD::encode (new DencMut)", |b| {
        b.iter(|| {
            let mut buf = BytesMut::with_capacity(16);
            <UuidD as DencMut>::encode(&uuid, &mut buf, 0).unwrap();
            let _ = black_box(buf.freeze());
        });
    });
}

fn bench_osdinfo_old(c: &mut Criterion) {
    let osdinfo = OsdInfo {
        last_clean_begin: 100,
        last_clean_end: 200,
        up_from: 300,
        up_thru: 400,
        down_at: 500,
        lost_at: 600,
    };

    c.bench_function("OsdInfo::encode (old Denc)", |b| {
        b.iter(|| {
            let _ = black_box(<OsdInfo as Denc>::encode(&osdinfo, 0).unwrap());
        });
    });
}

fn bench_osdinfo_new(c: &mut Criterion) {
    let osdinfo = OsdInfo {
        last_clean_begin: 100,
        last_clean_end: 200,
        up_from: 300,
        up_thru: 400,
        down_at: 500,
        lost_at: 600,
    };

    c.bench_function("OsdInfo::encode (new DencMut)", |b| {
        b.iter(|| {
            let mut buf = BytesMut::with_capacity(25);
            <OsdInfo as DencMut>::encode(&osdinfo, &mut buf, 0).unwrap();
            let _ = black_box(buf.freeze());
        });
    });
}

// Benchmark encoding a vector of types (composite benchmark)
fn bench_vector_old(c: &mut Criterion) {
    let items: Vec<PgId> = (0..100)
        .map(|i| PgId {
            pool: i as u64,
            seed: (i * 2) as u32,
        })
        .collect();

    c.bench_function("Vec<PgId>::encode (old Denc)", |b| {
        b.iter(|| {
            let _ = black_box(<Vec<PgId> as Denc>::encode(&items, 0).unwrap());
        });
    });
}

fn bench_vector_new(c: &mut Criterion) {
    let items: Vec<PgId> = (0..100)
        .map(|i| PgId {
            pool: i as u64,
            seed: (i * 2) as u32,
        })
        .collect();

    c.bench_function("Vec<PgId>::encode (new DencMut)", |b| {
        b.iter(|| {
            // Optimal preallocation: 4 bytes (length) + 100 * 17 bytes (items)
            let mut buf = BytesMut::with_capacity(4 + 100 * 17);
            <Vec<PgId> as DencMut>::encode(&items, &mut buf, 0).unwrap();
            let _ = black_box(buf.freeze());
        });
    });
}

criterion_group!(
    benches,
    bench_pgid_old,
    bench_pgid_new,
    bench_eversion_old,
    bench_eversion_new,
    bench_utime_old,
    bench_utime_new,
    bench_uuidd_old,
    bench_uuidd_new,
    bench_osdinfo_old,
    bench_osdinfo_new,
    bench_vector_old,
    bench_vector_new,
);

criterion_main!(benches);
