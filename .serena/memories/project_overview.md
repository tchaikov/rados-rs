# RADOS-RS Project Overview

## Purpose
RADOS-RS is a Rust native implementation of the RADOS (Reliable Autonomic Distributed Object Store) client library for connecting to Ceph storage clusters. It aims to provide modern async/await access to Ceph with no C library dependencies.

## Tech Stack
- **Language**: Rust (2021 edition)
- **Async Runtime**: Tokio 
- **Key Libraries**: bytes, thiserror, anyhow, tracing, rand, crc32c
- **Architecture**: Workspace with multiple crates
  - `denc`: Ceph encoding/decoding
  - `msgr2`: Messenger protocol v2 implementation
  - `hello-test`: Testing utilities

## Current Status
- Basic messenger protocol v2 implementation in progress
- HELLO frame exchange working
- AUTH frame implementation pending (recent work focus)
- Authentication system partially implemented
- Monitor client and OSD operations not yet implemented

## Key Resources
- Local Ceph cluster for testing at `/home/kefu/dev/rust-app-ceres/docker/ceph-config/`
- Ceph source code at `~/dev/ceph` for reference
- Protocol documentation at `~/dev/ceph/doc/dev/msgr2.rst`