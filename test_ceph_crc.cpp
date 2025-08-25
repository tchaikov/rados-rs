#include <stdio.h>
#include <stdint.h>
#include <string.h>

// Simplified from Ceph's crc32c.h
extern "C" uint32_t ceph_crc32c_func(uint32_t crc, const unsigned char *data, unsigned length);

static inline uint32_t ceph_crc32c(uint32_t crc, const unsigned char *data, unsigned length)
{
    return ceph_crc32c_func(crc, data, length);
}

int main() {
    // Test with some sample data (20 bytes - our HELLO segment)
    unsigned char data[20] = {
        0x01, 0x01, 0x01, 0x01, 0x0c, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00
    };

    printf("Testing Ceph CRC32C with 20 bytes\n");
    printf("Data: ");
    for (int i = 0; i < 20; i++) {
        printf("%02x", data[i]);
    }
    printf("\n\n");

    uint32_t crc1 = ceph_crc32c(0, data, 20);
    printf("ceph_crc32c(0, data, 20)          = %u (0x%08x)\n", crc1, crc1);

    uint32_t crc2 = ceph_crc32c(0xFFFFFFFF, data, 20);
    printf("ceph_crc32c(0xFFFFFFFF, data, 20) = %u (0x%08x)\n", crc2, crc2);

    uint32_t crc3 = ~ceph_crc32c(0xFFFFFFFF, data, 20);
    printf("~ceph_crc32c(0xFFFFFFFF, data, 20) = %u (0x%08x)\n", crc3, crc3);

    return 0;
}
