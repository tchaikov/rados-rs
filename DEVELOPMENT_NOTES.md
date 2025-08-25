# Development Notes

## Ceph Official Client Usage

The official Ceph client requires specifying the ceph.conf location to work properly:

```bash
LD_PRELOAD=/usr/lib/libasan.so.8 /home/kefu/dev/ceph/build/bin/ceph --conf "/home/kefu/dev/rust-app-ceres/docker/ceph-config/ceph.conf" -s 2>/dev/null
```

- `LD_PRELOAD=/usr/lib/libasan.so.8`: Uses AddressSanitizer
- `--conf`: Specifies the configuration file path
- `-s`: Status output flag
- `2>/dev/null`: Suppresses stderr output