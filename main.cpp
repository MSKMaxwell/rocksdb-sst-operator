#include "sst.h"
#include <filesystem>
#include <iostream>

const uint32_t buffer_size = 128;
static const uint64_t kNewFooterSize = 53;

// use std=c++17
std::filesystem::path sst_path("test.sst");
void newApproach();
void raw_code_read_sst();

#define NEW
int main()
{
    // SST sst("full.sst");
    // sst.decode();
    // for (int i = 0; i < sst.data_block[0].kv.size(); i++)
    // {
    //     std::cout << sst.data_block[0].kv[i].first.toString() << " " << sst.data_block[0].kv[i].second.toString() << std::endl;
    // }
    DataBlock datablock;
    for(int i = 1;i<50;++i){
        std::string key = std::to_string(i),value("value");
        value+=key;
        datablock.putkv(Slice(key),Slice(value));
    }
    int fd = open("write_datablock_test", O_WRONLY | O_CREAT, 0644);
    datablock.put_to_fd(fd, 0);
    return 0;
}

void newApproach()
{
    freopen("output.txt", "w", stdout);
    int fd = open("./test.sst", O_RDONLY);
    uint64_t sst_file_size = std::filesystem::file_size(sst_path);
    Buf footer_buf, index_block_buf, data_block_buf;
    Footer footer;
    footer.get(fd, sst_file_size, footer_buf);
    IndexBlock index_block;
    index_block.get(fd, footer.index_handle, index_block_buf);
    for (auto &data_block_handle : index_block.handles)
    {
        DataBlock data_block;
        data_block.get(fd, data_block_handle, data_block_buf);
        for (auto &kv : data_block.kv)
        {
            std::cout << kv.first.toString() << " " << kv.second.toString() << std::endl;
        }
    }
}

void raw_code_read_sst()
{
    freopen("output.txt", "w", stdout);
    char *p, *limit;
    // open sst file
    uint64_t sst_file_size = std::filesystem::file_size(sst_path);
    error_t err;
    int fd = open("./test1.sst", O_RDONLY);
    PERROR(opening file, fd);

    // read footer
    ssize_t read_bytes;
    char footer_buf[kNewFooterSize];
    read_bytes = pread(fd, footer_buf, kNewFooterSize, sst_file_size - kNewFooterSize);
    PERROR(reading footer, read_bytes);
    // build index handles
    Index_Handle meta_index_handle, index_handle;
    p = footer_buf + 1;
    limit = footer_buf + 53;
    p = GetVarint64Ptr(p, limit, &meta_index_handle.offset);
    p = GetVarint64Ptr(p, limit, &meta_index_handle.size);
    p = GetVarint64Ptr(p, limit, &index_handle.offset);
    p = GetVarint64Ptr(p, limit, &index_handle.size);

    // read block handles
    char block_handle_buf[index_handle.size], *block_handle_buf_p, *block_handle_buf_limit;
    read_bytes = pread(fd, block_handle_buf, index_handle.size, index_handle.offset);
    PERROR(reading index block, read_bytes);
    // restart_point
    block_handle_buf_p = block_handle_buf + index_handle.size - sizeof(uint32_t);
    block_handle_buf_limit = block_handle_buf + index_handle.size;
    uint32_t restart_point_num;
    GetBytesPtr(block_handle_buf_p, block_handle_buf_limit, reinterpret_cast<char *>(&restart_point_num), sizeof(uint32_t));
    uint32_t restart_point[restart_point_num];
    block_handle_buf_p = block_handle_buf + index_handle.size - sizeof(uint32_t) * (restart_point_num + 1);
    block_handle_buf_limit = block_handle_buf + index_handle.size - sizeof(uint32_t);
    for (int i = 0; i < restart_point_num; i++)
    {
        block_handle_buf_p = GetBytesPtr(block_handle_buf_p, block_handle_buf_limit, reinterpret_cast<char *>(&restart_point[i]), sizeof(uint32_t));
    }
    // extract one blockhandle until the end of block handle buffer
    block_handle_buf_p = block_handle_buf;
    block_handle_buf_limit = block_handle_buf + index_handle.size - sizeof(uint32_t) * (restart_point_num + 1);
    while (block_handle_buf_p < block_handle_buf_limit)
    {

        Block_Handle data_block_handle;
        unsigned shared_key_length, unshared_key_length;
        // data block handle key(used to seperate data blocks)
        char key1[buffer_size];
        block_handle_buf_p = GetBytesPtr(block_handle_buf_p, block_handle_buf_limit, reinterpret_cast<char *>(&shared_key_length), 1);
        block_handle_buf_p = GetBytesPtr(block_handle_buf_p, block_handle_buf_limit, reinterpret_cast<char *>(&unshared_key_length), 1);
        block_handle_buf_p = GetBytesPtr(block_handle_buf_p, block_handle_buf_limit, reinterpret_cast<char *>(&key1), shared_key_length + unshared_key_length);
        block_handle_buf_p = GetVarint32Ptr(block_handle_buf_p, block_handle_buf_limit, &data_block_handle.offset);
        block_handle_buf_p = GetVarint32Ptr(block_handle_buf_p, block_handle_buf_limit, &data_block_handle.size);

        // read a data block
        char data_block_buf[data_block_handle.size], *data_block_buf_p, *data_block_buf_limit;
        read_bytes = pread(fd, data_block_buf, data_block_handle.size, data_block_handle.offset);
        // read data block restart point
        uint32_t data_block_restart_point_num;
        data_block_buf_p = data_block_buf + data_block_handle.size - sizeof(uint32_t);
        data_block_buf_limit = data_block_buf + data_block_handle.size;
        GetBytesPtr(data_block_buf_p, data_block_buf_limit, reinterpret_cast<char *>(&data_block_restart_point_num), sizeof(uint32_t));
        uint32_t data_block_restart_point[data_block_restart_point_num];
        data_block_buf_p = data_block_buf + data_block_handle.size - sizeof(uint32_t) * (data_block_restart_point_num + 1);
        data_block_buf_limit = data_block_buf + data_block_handle.size - sizeof(uint32_t);
        for (int i = 0; i < data_block_restart_point_num; i++)
        {
            data_block_buf_p = GetBytesPtr(data_block_buf_p, data_block_buf_limit, reinterpret_cast<char *>(&data_block_restart_point[i]), sizeof(uint32_t));
        }
        // start to read KV-pairs in a data block
        unsigned s_key_length, uns_key_length, value_length;
        char key2[buffer_size], value[buffer_size], *new_key_start;
        memset(key2, 0, buffer_size);
        data_block_buf_p = data_block_buf;
        data_block_buf_limit = data_block_buf + data_block_handle.size - sizeof(uint32_t) * (data_block_restart_point_num + 1);
        while (data_block_buf_p < data_block_buf_limit)
        {
            data_block_buf_p = GetVarint32Ptr(data_block_buf_p, data_block_buf_limit, &s_key_length);
            new_key_start = key2 + s_key_length;
            data_block_buf_p = GetVarint32Ptr(data_block_buf_p, data_block_buf_limit, &uns_key_length);
            data_block_buf_p = GetVarint32Ptr(data_block_buf_p, data_block_buf_limit, &value_length);
            data_block_buf_p = GetBytesPtr(data_block_buf_p, data_block_buf_limit, new_key_start, uns_key_length);
            memset(value, 0, buffer_size);
            data_block_buf_p = GetBytesPtr(data_block_buf_p, data_block_buf_limit, reinterpret_cast<char *>(&value), value_length);
            for (int i = 0; i < s_key_length + uns_key_length; ++i)
            {
                if (key2[i] == 1)
                {
                    key2[i] = '\0';
                    break;
                }
            }
            // in dictionary order
            printf("%s %s\n", key2, value);
            // sleep for 100 ms
            // usleep(20000);
        }
    }
}