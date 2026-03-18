- 使用env ASAN_OPTIONS=detect_odr_violation=0,detect_leaks=0 CEPH_LIB=$HOME/dev/ceph/build/lib ~/dev/ceph/build/bin/ceph-dencoder type pg_pool_t import ~/dev/ceph/ceph-object-corpus/archive/19.2.0-404-g78ddc7f9027/objects/pg_pool_t/453c7bee75dca4766602cee267caa589 decode dump_json可以以 json 格式 dump pg_pool_t 的内容
- 在本地 ~/dev/ceph 有 ceph 的源代码
- ~/dev/ceph/src/include/denc.h 是 ceph 编码解码的实现
- ~/dev/ceph/ceph-object-corpus/archive/19.2.0-404-g78ddc7f9027/objects 下保存着很多 ceph 类型 encode 得到的二进制文件，我们把它们叫做 corpus
- 这个项目的目标是用 rust 实现部分 ceph 类型的编解码功能，最终为 librados 的 rust 实现做准备。
- 当本项目无法正常解码的时候，我们可以用 ~/dev/ceph/build/bin/ceph-dencoder 来分析 corpus 的内容，提供参考。
- OSDMap 的定义在 ~/dev/ceph/src/osd/OSDMap.h 和 ~/dev/ceph/src/osd/OSDMap.cc 里

