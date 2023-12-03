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
const int sst_size_max = 1024 * 1024 * 16;
const int fixed_block_buffer_size = 1024 * 4;
const int restart_point_max = 100;
const int restart = 16;
// whole sst buffer
char sst_buffer[sst_size_max], index_block_buffer[fixed_block_buffer_size];
uint32_t data_block_restart_point[restart_point_max], index_block_restart_point[restart_point_max * restart_point_max];
uint32_t data_block_restart_point_num, index_block_restart_point_num;
char *data_block_head, *sst_pointer, *index_block_pointer;

char *put_kv_remove_prefix(std::string &key, std::string &pre_key, std::string &value, int type, char *sst_pointer);
void getKV();
char *putKV(std::string &key, std::string &value);
char *putRestartPoint(uint32_t *restart_point, uint32_t &restart_point_num, char *sst_pointer);
char *putBlockHandle(std::string &key, BlockHandle value);
void to_string(std::string &str, BlockHandle &value);
char *putFooter(uint64_t index_block_offset, uint64_t index_block_size, uint64_t metaindex_block_offset, uint64_t metaindex_block_size);

int main()
{
    // freopen("output1.txt", "w", stdout);
    int fd = open("test_write.sst", O_WRONLY | O_CREAT, 0644);
    // assume two buffers, one to save data block, one to save index block
    // require two BlockHandles, one per buffer(block)
    data_block_head = sst_buffer;
    sst_pointer = sst_buffer;
    index_block_pointer = index_block_buffer;

    // kv-pair <string,string>
    // internal key = user key + type + sequence number
    const uint8_t kTypeDeletion = 0x0;
    const uint8_t kTypeValue = 0x1;
    const uint8_t kTypeMerge = 0x2;
    const uint8_t kTypeSingleDeletion = 0x7;
    const uint8_t kTypeDeletionWithTimestamp = 0x14;
    std::string key, value;
    for (int i = 0; i < 20; i++)
    {
        key = std::to_string(i);
        value = std::string("test_data") + std::to_string(i);
        putKV(key, value);
    }
    // finish kv-pair input

    // process last data block
    // write restart point array
    sst_pointer = putRestartPoint(data_block_restart_point, data_block_restart_point_num, sst_pointer);
    // write BlockHandle
    BlockHandle last;
    last.offset = data_block_head - sst_buffer;
    last.size = sst_pointer - data_block_head;
    index_block_pointer = putBlockHandle(key, last);
    // TODO xxh3

    uint64_t index_block_offset = sst_pointer - sst_buffer;
    uint64_t index_block_size = 0;
    uint64_t meta_index_block_offset = 0;
    uint64_t meta_index_block_size = 0;

    // write index block
    // write restart point array
    index_block_pointer = putRestartPoint(index_block_restart_point, index_block_restart_point_num, index_block_pointer);
    memcpy(sst_pointer, index_block_buffer, index_block_pointer - index_block_buffer);
    sst_pointer += index_block_pointer - index_block_buffer;
    index_block_size = index_block_pointer - index_block_buffer;
    // TODO xxh3

    // TODO metablocks(properties, a special datablock)

    // write footer
    sst_pointer = putFooter(index_block_offset, index_block_size, meta_index_block_offset, meta_index_block_size);
    pwrite(fd, sst_buffer, sst_pointer - sst_buffer, 0);
    return 0;
}

char *putKV(std::string &key, std::string &value)
{
    static std::string pre_key;
    static uint32_t restart_point_count = 0;
    static uint32_t entry_size = 0;

    // new data block
    if (sst_pointer == data_block_head)
    {
        // clear restart point array
        memset(data_block_restart_point, 0, sizeof(data_block_restart_point));
        data_block_restart_point_num = 1;
        data_block_restart_point[0] = 0;
        entry_size = 1;
        pre_key.clear();
        sst_pointer = put_kv_remove_prefix(key, pre_key, value, 0, sst_pointer);
        pre_key = key;
        return sst_pointer;
    }

    // check if data block full and create a new one
    if (sst_pointer + key.size() + value.size() + 30 - data_block_head >= fixed_block_buffer_size)
    {
        // save restart point
        data_block_restart_point[data_block_restart_point_num] = sst_pointer - data_block_head;
        data_block_restart_point_num++;
        // put restart point
        sst_pointer = putRestartPoint(data_block_restart_point, data_block_restart_point_num, sst_pointer);
        // TODO xxh3
        // use pre_key and sst_pointer to insert a index_block kv-pair
        BlockHandle data_block_handle;
        data_block_handle.offset = data_block_head - sst_buffer;
        data_block_handle.size = sst_pointer - data_block_head;
        index_block_pointer = putBlockHandle(pre_key, data_block_handle);
        // create new data block
        data_block_head = sst_pointer;
        return putKV(key, value);
    }

    // put kv pair
    // check if need restart
    if (entry_size % restart == 0)
    {
        data_block_restart_point[data_block_restart_point_num] = sst_pointer - data_block_head;
        data_block_restart_point_num++;
        pre_key.clear();
    }
    sst_pointer = put_kv_remove_prefix(key, pre_key, value, 0, sst_pointer);
    pre_key = key;
    entry_size++;
    return sst_pointer;
}

char *putFooter(uint64_t index_block_offset, uint64_t index_block_size, uint64_t metaindex_block_offset, uint64_t metaindex_block_size)
{
    static uint64_t kMagicNumber = 9863518390377041911ULL;
    static uint32_t kExtendedMagicNumber = 7995454;
    static uint64_t kNewFooterSize = 53;
    static uint64_t kXXH3checksum = 4;
    uint32_t version = 5;
    // assume version 1-5
    // put checksum type
    sst_pointer = EncodeVarint64(sst_pointer, kXXH3checksum);
    // part begin
    char *begin = sst_pointer;
    // put metaindex handle
    sst_pointer = EncodeVarint64(sst_pointer, metaindex_block_offset);
    sst_pointer = EncodeVarint64(sst_pointer, metaindex_block_size);
    // put index handle
    sst_pointer = EncodeVarint64(sst_pointer, index_block_offset);
    sst_pointer = EncodeVarint64(sst_pointer, index_block_size);
    // put zero padding
    while (sst_pointer < begin + 40)
    {
        *(sst_pointer) = 0;
        ++sst_pointer;
    }
    // part end
    // put format version
    sst_pointer = PutBytesPtr(sst_pointer, reinterpret_cast<char *>(&version), sizeof(uint32_t));
    // put magic number
    uint64_t magic = kMagicNumber;
    sst_pointer = PutBytesPtr(sst_pointer, reinterpret_cast<char *>(&magic), sizeof(uint64_t));
    return sst_pointer;
}

char *put_kv_remove_prefix(std::string &key, std::string &pre_key, std::string &value, int type, char *block_pointer)
{
    // remove prefix
    int i = 0;
    for (; i < key.size() && i < pre_key.size(); i++)
    {
        if (key[i] != pre_key[i])
            break;
    }
    // put key
    // shared | unshared | value | key | value
    if (type == 0)
    {
        block_pointer = EncodeVarint32(block_pointer, i);
        block_pointer = EncodeVarint32(block_pointer, key.size() - i);
        block_pointer = EncodeVarint32(block_pointer, value.size());
        block_pointer = PutBytesPtr(block_pointer, key.c_str() + i, key.size() - i);
        block_pointer = PutBytesPtr(block_pointer, value.c_str(), value.size());
    }
    else
    {
        // shared | unshared | key | value
        block_pointer = EncodeVarint32(block_pointer, i);
        block_pointer = EncodeVarint32(block_pointer, key.size() - i);
        block_pointer = PutBytesPtr(block_pointer, key.c_str() + i, key.size() - i);
        block_pointer = PutBytesPtr(block_pointer, value.c_str(), value.size());
    }
    return block_pointer;
}

char *putRestartPoint(uint32_t *restart_point, uint32_t &restart_point_num, char *sst_pointer)
{
    // put restart point
    // restart point array
    for (int i = 0; i < restart_point_num; i++)
    {
        sst_pointer = PutBytesPtr(sst_pointer, reinterpret_cast<char *>(&restart_point[i]), sizeof(uint32_t));
    }
    // restart point num
    sst_pointer = PutBytesPtr(sst_pointer, reinterpret_cast<char *>(&restart_point_num), sizeof(uint32_t));
    return sst_pointer;
}

// for index block
char *putBlockHandle(std::string &key, BlockHandle value)
{
    // new index block
    static std::string pre_key;
    static uint32_t entry_size = 0;
    static std::string value_str;

    if (index_block_pointer == index_block_buffer)
    {
        memset(index_block_restart_point, 0, sizeof(index_block_restart_point));
        index_block_restart_point_num = 1;
        index_block_restart_point[0] = 0;
        entry_size = 1;
        pre_key.clear();
        to_string(value_str, value);
        index_block_pointer = put_kv_remove_prefix(key, pre_key, value_str, 1, index_block_pointer);
        pre_key = key;
        return index_block_pointer;
    }

    // index block is unique

    if (entry_size % restart == 0)
    {
        index_block_restart_point[index_block_restart_point_num] = index_block_pointer - index_block_buffer;
        index_block_restart_point_num++;
        pre_key.clear();
    }

    to_string(value_str, value);
    index_block_pointer = put_kv_remove_prefix(key, pre_key, value_str, 1, index_block_pointer);
    pre_key = key;
    entry_size++;
    return index_block_pointer;
}

// write BlockHandlce into std::string
void to_string(std::string &str, BlockHandle &value)
{
    static char buf[10];
    static char *p;
    p = buf;
    p = EncodeVarint32(p, value.offset);
    p = EncodeVarint32(p, value.size);
    str.resize(p - buf);
    memcpy(str.data(), buf, p - buf);
    return;
}
