## Ceph encoding

- 使用env ASAN_OPTIONS=detect_odr_violation=0,detect_leaks=0 CEPH_LIB=$HOME/dev/ceph/build/lib ~/dev/ceph/build/bin/ceph-dencoder type pg_pool_t import ~/dev/ceph/ceph-object-corpus/archive/19.2.0-404-g78ddc7f9027/objects/pg_pool_t/453c7bee75dca4766602cee267caa589 decode dump_json可以以 json 格式 dump pg_pool_t 的内容
- ~/dev/ceph/src/include/denc.h 是 ceph 编码解码的实现
- ~/dev/ceph/ceph-object-corpus/archive/19.2.0-404-g78ddc7f9027/objects 下保存着很多 ceph 类型 encode 得到的二进制文件，我们把它们叫做 corpus
- 这个项目的目标是用 rust 实现 librados。
- 当本项目无法正常解码的时候，我们可以用 "cd ~/dev/ceph/build; bin/ceph-dencoder" 来分析 corpus 的内容，提供参考。
- OSDMap 的定义在 ~/dev/ceph/src/osd/OSDMap.h 和 ~/dev/ceph/src/osd/OSDMap.cc 里
- 我们在编码时当时采用的 features 可能会影响编码的结果，这是除了 Sized 的另外一种和 Denc 相关的编译期属性，我们也许也可以用 trait 表示，因为有的类型的编码会受到 features 影响，而有的不会。请注意，那些会被 features 影响的类型会使用 WRITE_CLASS_ENCODER_FEATURES 来对这种行为进行标注。

## local resources

- ceph's source code is located at ~/dev/ceph
- the official document for the messenger v2 protocol is located at ~/dev/ceph/doc/dev/msgr2.rst
- the msgr2 protocol is implemented in following places
  - ~/dev/ceph/src/crimson/net/ProtocolV2.cc
  - ~/dev/ceph/src/crimson/net/ProtocolV2.h
  - ~/dev/ceph/src/crimson/net/FrameAssemblerV2.cc
  - ~/dev/ceph/src/crimson/net/FrameAssemblerV2.h
  - ~/dev/ceph/src/msg/async/ProtocolV2.cc
  - ~/dev/ceph/src/msg/async/ProtocolV2.h
  - ~/dev/ceph/src/msg/async/frames_v2.cc
  - ~/dev/ceph/src/msg/async/frames_v2.h
  
## cluster for testing

- there is a cluster running locally. it's setting is located at `/home/kefu/dev/ceph/build/ceph.conf`, use `cd ~/dev/ceph/build; ../src/vstart.sh -d --without-dashboard` to start it, and use `cd ~/dev/ceph/build; ../src/stop.sh` to stop it
- please use `LD_PRELOAD=/usr/lib/libasan.so.8 /home/kefu/dev/ceph/build/bin/ceph --conf "/home/kefu/dev/ceph/build/ceph.conf" -s 2>/dev/null` to run the offical client to verify the behavior, or when we need to dump the TCP traffic to cross check with our own implementation`

## guidelines

- do not start from scratch.
- always fix existing code
- test encoding using dencoder in this project, and verify it using ceph-dencoder with the corpus fro ceph project
