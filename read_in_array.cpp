// Get functions
#include "include/coding.h"
// BlockHandle
#include "include/format.h"
#include <fcntl.h>
#include <unistd.h>
#include <iostream>
#include <memory>
#include <vector>
// require std=c++17
#include <filesystem>

// read raw sst into an array and decode
// raw code
int main()
{
    freopen("./output/output1.txt", "w", stdout);
    // open a sst and allocate a buffer with sst size
    int fd = open("test.sst", O_RDONLY);
    uint64_t size = std::filesystem::file_size("test.sst");
    std::unique_ptr<char[]> buf(new char[size]);
    pread(fd, buf.get(), size, 0);

    // file internal pointer
    char *p, *limit;
    // decode
    // decode
    // read footer code, copied from format.h
    const uint64_t footer_size = 53;
    BlockHandle metaindex_handle,index_handle;
    p = buf.get() + size - footer_size + 1;
    limit = buf.get() + size;
    // in the future we may need to inspect into meta blocks
    // jump for now
    p = GetVarint64Ptr(p, limit, &metaindex_handle.offset);
    p = GetVarint64Ptr(p, limit, &metaindex_handle.size);
    p = GetVarint64Ptr(p, limit, &index_handle.offset);
    p = GetVarint64Ptr(p, limit, &index_handle.size);

    // read index block to find data block handles
    // read restart point and jump restart point array
    p = buf.get() + index_handle.offset + index_handle.size - 4;
    limit = buf.get() + index_handle.offset + index_handle.size;
    uint32_t restart_point_num;
    p = GetBytesPtr(p, limit, reinterpret_cast<char *>(&restart_point_num), sizeof(uint32_t));
    // read data block handles
    const uint32_t kShared_key_length = 128;
    std::vector<BlockHandle> data_block_handles;
    p = buf.get() + index_handle.offset;
    limit = buf.get() + index_handle.offset + index_handle.size - (1 + restart_point_num) * sizeof(uint32_t);
    std::unique_ptr<char[]> shared_key_buf(new char[kShared_key_length]);
    do
    {
        BlockHandle data_block_handle;
        uint32_t shared_key_length, unshared_key_length;
        // data block handle key(used to seperate data blocks)
        p = GetVarint32Ptr(p, limit, &shared_key_length);
        p = GetVarint32Ptr(p, limit, &unshared_key_length);
        p = GetBytesPtr(p, limit, reinterpret_cast<char *>(shared_key_buf.get() + shared_key_length), unshared_key_length);
        // get handle
        p = GetVarint64Ptr(p, limit, &data_block_handle.offset);
        p = GetVarint64Ptr(p, limit, &data_block_handle.size);
        data_block_handles.push_back(data_block_handle);
    } while (p < limit);

    // read data block
    DataBlock data_block;
    for (auto data_block_handle : data_block_handles)
    {
        p = buf.get() + data_block_handle.offset + data_block_handle.size - sizeof(uint32_t);
        limit = buf.get() + data_block_handle.offset + data_block_handle.size;
        // read restart point
        uint32_t restart_point_num;
        p = GetBytesPtr(p, limit, reinterpret_cast<char *>(&restart_point_num), sizeof(uint32_t));
        // read kv pairs
        p = buf.get() + data_block_handle.offset;
        limit = buf.get() + data_block_handle.offset + data_block_handle.size - (1 + restart_point_num) * sizeof(uint32_t);
        std::unique_ptr<char[]> shared_key_buf(new char[kShared_key_length]);
        do
        {
            uint32_t shared_key_length, unshared_key_length, value_length;
            p = GetVarint32Ptr(p, limit, &shared_key_length);
            p = GetVarint32Ptr(p, limit, &unshared_key_length);
            p = GetVarint32Ptr(p, limit, &value_length);
            // read key and value
            p = GetBytesPtr(p, limit, reinterpret_cast<char *>(shared_key_buf.get() + shared_key_length), unshared_key_length);
            // read value
            std::string key(shared_key_buf.get(), shared_key_length + unshared_key_length), value(p, value_length);
            p += value_length;
            // output key and value
            std::cout << key << " " << value << std::endl;
        } while (p < limit);
    }
}