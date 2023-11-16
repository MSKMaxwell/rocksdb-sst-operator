#include "coding.h"
#include <vector>
#include <memory>
#include <string.h>
#include <fcntl.h>
#include <unistd.h>

#ifndef _FORMAT_H_
#define _FORMAT_H_

struct Slice
{
    std::unique_ptr<char[]> data;
    uint32_t size;
    Slice(char *p, uint32_t size)
    {
        this->data = std::make_unique<char[]>(size);
        memcpy(this->data.get(), p, size);
        this->size = size;
    }
    Slice (std::string str)
    {
        this->data = std::make_unique<char[]>(str.size());
        memcpy(this->data.get(), str.c_str(), str.size());
        this->size = str.size();
    }
    Slice(uint32_t size)
    {
        this->data = std::make_unique<char[]>(size);
        this->size = size;
    }
    void put(char *src)
    {
        memcpy(this->data.get(), src, this->size);
    }
    char *get()
    {
        return this->data.get();
    }
    std::string toString()
    {
        return std::string(this->data.get(), this->size);
    }
    size_t difference_offset(const Slice &b) const
    {
        size_t off = 0;
        const size_t len = (size < b.size) ? size : b.size;
        for (; off < len; off++)
        {
            if (data[off] != b.data[off])
                break;
        }
        return off;
    }
};

struct Buf
{
    std::unique_ptr<char[]> buffer;
    char *p, *limit;
    uint32_t size;
    Buf(uint32_t size = 1024)
    {
        this->buffer = std::make_unique<char[]>(size);
        this->p = buffer.get();
        this->size = size;
        this->limit = buffer.get() + size;
    }
    void resize(uint32_t new_size)
    {
        this->size = new_size;
        buffer.reset(new char[this->size]);
        this->p = buffer.get();
        this->limit = buffer.get() + size;
    }
};

struct Index_Handle
{
    uint64_t offset;
    uint64_t size;
};

struct Block_Handle
{
    uint32_t offset;
    uint32_t size;
};
/*
**Footer@V0：**
- metaindex handle (varint64 offset, varint64 size)
- index handle (varint64 offset, varint64 size)
- zero padding,this part shoule get a size of 40 byte.
- legacy magic number (8 bytes)

共48byte
Version@0中的Footer没有版本号，需要从magic number是否lagacy推测

**Footer@V1:5：**
- checksum type (char, 1 byte)
- metaindex handle (varint64 offset, varint64 size)
- index handle (varint64 offset, varint64 size)
- zero padding,this part shoule get a size of 40 byte.
- format_version (uint32LE, 4 bytes), also called "footer version"
- newer magic number (8 bytes)

共53byte

**Footer@V6:X：**
- checksum type (char, 1 byte)
- extended magic number (4 bytes) = 0x3e 0x00 0x7a 0x00(little endian)
- footer_checksum (uint32LE, 4 bytes)
- base_context_checksum (uint32LE, 4 bytes)
- metaindex block size (uint32LE, 4 bytes)
- zero padding (24 bytes, reserved for future use)
- format_version (uint32LE, 4 bytes), also called "footer version"
- newer magic number (8 bytes)
*/

// const uint64_t kMagicNumber = 0x88e241b785f4cff7;
struct Footer
{
    static const uint64_t kMagicNumber = 9863518390377041911ULL;
    static const uint32_t kExtendedMagicNumber = 7995454;
    static const uint64_t kNewFooterSize = 53;
    Index_Handle meta_index_handle, index_handle;
    uint32_t version;
    void get(int fd, uint64_t sst_file_size, Buf &buf)
    {
        // assume version 1-5
        ssize_t read_bytes;
        buf.resize(kNewFooterSize);
        read_bytes = pread(fd, buf.buffer.get(), kNewFooterSize, sst_file_size - kNewFooterSize);
        PERROR(reading footer, read_bytes);
        // build index handles
        // + 1 to jump over checksum type
        buf.p = buf.buffer.get() + 1;
        buf.limit = buf.buffer.get() + 53;
        buf.p = GetVarint64Ptr(buf.p, buf.limit, &meta_index_handle.offset);
        buf.p = GetVarint64Ptr(buf.p, buf.limit, &meta_index_handle.size);
        buf.p = GetVarint64Ptr(buf.p, buf.limit, &index_handle.offset);
        buf.p = GetVarint64Ptr(buf.p, buf.limit, &index_handle.size);
    }
};

// may be changed in the future
const uint32_t kShared_key_length = 128;

struct IndexBlock
{
    std::vector<std::unique_ptr<char[]>> keys;
    std::vector<Block_Handle> handles;
    uint32_t restart_point_num, entry_size;
    std::vector<uint32_t> restart_point;

    void get(int fd, Index_Handle &index_handle, Buf &buf)
    {
        // init entry size
        entry_size = 0;

        // read index block into buffer
        buf.resize(index_handle.size);
        PERROR(reading index block, pread(fd, buf.buffer.get(), index_handle.size, index_handle.offset));

        // restart_point
        buf.p = buf.buffer.get() + index_handle.size - sizeof(uint32_t);
        buf.limit = buf.buffer.get() + index_handle.size;
        GetBytesPtr(buf.p, buf.limit, reinterpret_cast<char *>(&restart_point_num), sizeof(uint32_t));

        // build restart_point array
        restart_point.resize(restart_point_num);
        buf.p = buf.buffer.get() + index_handle.size - sizeof(uint32_t) * (restart_point_num + 1);
        buf.limit = buf.buffer.get() + index_handle.size - sizeof(uint32_t);
        for (int i = 0; i < restart_point_num; i++)
        {
            buf.p = GetBytesPtr(buf.p, buf.limit, reinterpret_cast<char *>(&restart_point[i]), sizeof(uint32_t));
        }

        // extract one blockhandle until the end of block handle buffer
        buf.p = buf.buffer.get();
        buf.limit = buf.buffer.get() + index_handle.size - sizeof(uint32_t) * (restart_point_num + 1);
        // TODO: use a better way to handle shared key
        std::unique_ptr<char[]> shared_key_buf;
        shared_key_buf = std::make_unique<char[]>(kShared_key_length);
        while (buf.p < buf.limit)
        {
            Block_Handle data_block_handle;
            uint32_t shared_key_length, unshared_key_length;
            // data block handle key(used to seperate data blocks)
            buf.p = GetVarint32Ptr(buf.p, buf.limit, &shared_key_length);
            buf.p = GetVarint32Ptr(buf.p, buf.limit, &unshared_key_length);
            buf.p = GetBytesPtr(buf.p, buf.limit, reinterpret_cast<char *>(shared_key_buf.get() + shared_key_length), unshared_key_length);
            keys.push_back(std::make_unique<char[]>(shared_key_length + unshared_key_length));
            memcpy(keys.back().get(), shared_key_buf.get(), shared_key_length + unshared_key_length);
            // get handle
            buf.p = GetVarint32Ptr(buf.p, buf.limit, &data_block_handle.offset);
            buf.p = GetVarint32Ptr(buf.p, buf.limit, &data_block_handle.size);
            handles.push_back(data_block_handle);
            // added one entry
            ++entry_size;
        }
    }
};

struct DataBlock
{
    static const ssize_t block_size = 4096;
    static const ssize_t restart_interval = 16;
    std::vector<std::pair<Slice, Slice>> kv;
    uint32_t restart_point_num, entry_size, kv_size;
    std::vector<uint32_t> restart_point;

    DataBlock()
    {
        kv_size = 0;
        entry_size = 0;
    }

    void putkv(Slice key, Slice value)
    {
        kv_size += key.size + value.size;
        kv.push_back(std::make_pair(std::move(key), std::move(value)));
        // kv.push_back(std::make_pair(key, value));
        ++entry_size;
    }

    void get(int fd, Block_Handle &data_block_handle, Buf &buf)
    {
        entry_size = 0;
        buf.resize(data_block_handle.size);
        // read into buffer
        PERROR(reading data block, pread(fd, buf.buffer.get(), data_block_handle.size, data_block_handle.offset));
        // read data block restart point
        buf.p = buf.buffer.get() + data_block_handle.size - sizeof(uint32_t);
        buf.limit = buf.buffer.get() + data_block_handle.size;
        GetBytesPtr(buf.p, buf.limit, reinterpret_cast<char *>(&restart_point_num), sizeof(uint32_t));
        // build restart_point array
        restart_point.resize(restart_point_num);
        buf.p = buf.buffer.get() + data_block_handle.size - sizeof(uint32_t) * (restart_point_num + 1);
        buf.limit = buf.buffer.get() + data_block_handle.size - sizeof(uint32_t);
        for (int i = 0; i < restart_point_num; i++)
        {
            buf.p = GetBytesPtr(buf.p, buf.limit, reinterpret_cast<char *>(&restart_point[i]), sizeof(uint32_t));
        }
        // extract one blockhandle until the end of block handle buffer
        buf.p = buf.buffer.get();
        buf.limit = buf.buffer.get() + data_block_handle.size - sizeof(uint32_t) * (restart_point_num + 1);
        std::unique_ptr<char[]> shared_key_buf;
        // TODO: use a better way to handle shared key
        shared_key_buf = std::make_unique<char[]>(kShared_key_length);
        while (buf.p < buf.limit)
        {
            uint32_t shared_key_length, unshared_key_length, value_length;
            // data block handle key(used to seperate data blocks)
            buf.p = GetVarint32Ptr(buf.p, buf.limit, &shared_key_length);
            buf.p = GetVarint32Ptr(buf.p, buf.limit, &unshared_key_length);
            buf.p = GetVarint32Ptr(buf.p, buf.limit, &value_length);
            buf.p = GetBytesPtr(buf.p, buf.limit, reinterpret_cast<char *>(shared_key_buf.get() + shared_key_length), unshared_key_length);
            kv.push_back(std::make_pair(Slice(shared_key_buf.get(), shared_key_length + unshared_key_length), Slice(value_length)));
            buf.p = GetBytesPtr(buf.p, buf.limit, reinterpret_cast<char *>(kv.back().second.get()), value_length);

            ++entry_size;
        }
    }

    // write the data block to fd at given offset
    // return the offset of next block
    ssize_t put_to_fd(int fd, ssize_t offset)
    {
        size_t buf_size = estimated_size() * 2;
        // write the first kv
        Buf buf(buf_size);
        // shared  | unshard | value | key | value
        buf.p = EncodeVarint32(buf.p, 0);
        buf.p = EncodeVarint32(buf.p, kv[0].first.size);
        buf.p = EncodeVarint32(buf.p, kv[0].second.size);
        buf.p = PutBytesPtr(buf.p, kv[0].first.get(), kv[0].first.size);
        buf.p = PutBytesPtr(buf.p, kv[0].second.get(), kv[0].second.size);
        // write the rest kv
        unsigned restart_num = 1;
        restart_point.push_back(offset);
        for (uint32_t i = 1; i < kv.size(); ++i)
        {
            size_t shared_key_length, unshared_key_length;
            if (i % restart_interval == 0)
            {
                restart_point.push_back(buf.p - buf.buffer.get() + offset);
                ++restart_num;
                shared_key_length = 0;
                unshared_key_length = kv[i].first.size;
            }
            else
            {
                shared_key_length = kv[i].first.difference_offset(kv[i - 1].first);
                unshared_key_length = kv[i].first.size - shared_key_length;
            }
            buf.p = EncodeVarint32(buf.p, shared_key_length);
            buf.p = EncodeVarint32(buf.p, unshared_key_length);
            buf.p = EncodeVarint32(buf.p, kv[i].second.size);
            buf.p = PutBytesPtr(buf.p, kv[i].first.get() + shared_key_length, unshared_key_length);
            buf.p = PutBytesPtr(buf.p, kv[i].second.get(), kv[i].second.size);
        }
        // write the restart point array
        for(uint32_t i = 0; i < restart_point.size(); ++i){
            buf.p = PutBytesPtr(buf.p, reinterpret_cast<char *>(&restart_point[i]), sizeof(uint32_t));
        }
        buf.p = PutBytesPtr(buf.p, reinterpret_cast<char *>(&restart_num), sizeof(uint32_t));
        pwrite(fd, buf.buffer.get(), buf.p - buf.buffer.get(), offset);
        return offset + buf.p - buf.buffer.get();
    }

    ssize_t estimated_size()
    {
        ssize_t tsize = 0;
        // kv size
        // for (int i = 0; i < kv.size(); i++)
        // {
        //     tsize += kv[i].first.size + kv[i].second.size;
        // }
        tsize += kv_size;
        // restart point array size
        tsize += entry_size / 16 * 4;
        // restart point num
        ++tsize;
        // xxh3 checksum  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
        tsize += 5;
        return tsize;
    }
};

#endif