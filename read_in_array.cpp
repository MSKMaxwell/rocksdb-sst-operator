// GetVarint() GetBytes()
#include "include/coding.h"
// BlockHandle only
#include "include/format.h"
#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>
// cout
#include <iostream>

// read raw sst into an array and decode
// raw code
int main()
{
    freopen("output1.txt", "w", stdout);
    // open a sst and allocate a buffer with sst size
    int fd = open("tes t_write.sst", O_RDONLY);
    // get file size
    struct stat file_stat;
    fstat(fd, &file_stat);
    uint64_t size = file_stat.st_size;
    // set const variables and buffer
    const uint32_t kShared_key_length = 128;
    char buf[size], shared_key_buf[kShared_key_length];
    // read whole sst file into buffer
    pread(fd, buf, size, 0);

    // file internal pointer
    char *p, *limit;
    // decode
    // read footer
    const uint64_t footer_size = 53;
    BlockHandle metaindex_handle, index_handle;
    p = buf + size - footer_size + 1;
    limit = buf + size;
    // TODO metablocks
    // jump for now
    p = GetVarint64Ptr(p, limit, &metaindex_handle.offset);
    p = GetVarint64Ptr(p, limit, &metaindex_handle.size);
    p = GetVarint64Ptr(p, limit, &index_handle.offset);
    p = GetVarint64Ptr(p, limit, &index_handle.size);

    // read index block to find data block handles
    // read restart point and jump restart point array
    p = buf + index_handle.offset + index_handle.size - 4;
    limit = buf + index_handle.offset + index_handle.size;
    uint32_t restart_point_num;
    p = GetBytesPtr(p, limit, reinterpret_cast<char *>(&restart_point_num), sizeof(uint32_t));
    // outer loop
    // read data block handles
    std::vector<BlockHandle> data_block_handles;
    p = buf + index_handle.offset;
    limit = buf + index_handle.offset + index_handle.size - (1 + restart_point_num) * sizeof(uint32_t);
    do
    {
        BlockHandle data_block_handle;
        uint32_t shared_key_length, unshared_key_length;
        // data key (used to seperate data blocks)
        p = GetVarint32Ptr(p, limit, &shared_key_length);
        p = GetVarint32Ptr(p, limit, &unshared_key_length);
        p = GetBytesPtr(p, limit, reinterpret_cast<char *>(shared_key_buf + shared_key_length), unshared_key_length);
        // get handle
        p = GetVarint64Ptr(p, limit, &data_block_handle.offset);
        p = GetVarint64Ptr(p, limit, &data_block_handle.size);

        // inner loop
        // read data blocks
        // inner loop pointer
        char *p1, *limit1;

        // read restart point
        p1 = buf + data_block_handle.offset + data_block_handle.size - sizeof(uint32_t);
        limit1 = buf + data_block_handle.offset + data_block_handle.size;
        uint32_t restart_point_num;
        p1 = GetBytesPtr(p1, limit1, reinterpret_cast<char *>(&restart_point_num), sizeof(uint32_t));
        // set pointers to read kv pairs
        p1 = buf + data_block_handle.offset;
        limit1 = buf + data_block_handle.offset + data_block_handle.size - (1 + restart_point_num) * sizeof(uint32_t);
        do
        {
            uint32_t shared_key_length, unshared_key_length, value_length;
            p1 = GetVarint32Ptr(p1, limit1, &shared_key_length);
            p1 = GetVarint32Ptr(p1, limit1, &unshared_key_length);
            p1 = GetVarint32Ptr(p1, limit1, &value_length);
            // read key
            p1 = GetBytesPtr(p1, limit1, reinterpret_cast<char *>(shared_key_buf + shared_key_length), unshared_key_length);
            // read value
            std::string key(shared_key_buf, shared_key_length + unshared_key_length), value(p1, value_length);
            p1 += value_length;
            // ********************
            // output key and value
            std::cout << key << " " << value << std::endl;
            // ********************
            // TODO add key type detaction, key valivation
            // TEST
            // 设置两个不同的sst，都有prefix，一个是put生成的键值对，一个是delete生成的记录，尝试能否分别清楚是否put了和delete
            // 是就输出put或者delete
            // drop unnecessary keys
            // TODO XXH3 in hls
            // TODO properties (deep into rocksdb source code)
            // TODO bloom filter
            // TODO snappy hls
        } while (p1 < limit1);

    } while (p < limit);
}