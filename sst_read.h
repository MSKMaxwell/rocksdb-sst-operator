#include "coding.h"
#include "format.h"
#include <unistd.h>
#include <filesystem>
#include <fcntl.h>
#include <memory>

#ifndef _SST_READ_H_
#define _SST_READ_H_

void GetHandleFromBuf(Buf *buf, uint64_t *offset, uint64_t *size)
{
    char *limit = buf->buf.get() + buf->size;
    buf->p = GetVarint64Ptr(buf->p, limit, offset);
    buf->p = GetVarint64Ptr(buf->p, limit, size);
}
// TODO
// open a sst file and create a buffer, fill the buffer if buf->p < buf->size / 2
struct SST
{
    std::filesystem::path sst_path;
    int fd;
    const uint64_t kNewFooterSize = 53;
    uint64_t sst_file_size;
    Buf buffer;
    SST(std::filesystem::path sst_path)
        : buffer(1024 * 1024)
    {
        this->sst_path = sst_path;
        this->sst_file_size = std::filesystem::file_size(sst_path);
        fd = open(sst_path.c_str(), O_RDONLY);
    }
    void BuildFooter(Footer& footer){
        ssize_t read_bytes;
        read_bytes = pread(fd, buffer.buf.get(), kNewFooterSize, sst_file_size - kNewFooterSize + 1);
        assert(read_bytes == kNewFooterSize);
        char *p = buffer.buf.get() + 1, *limit = buffer.buf.get() + 53;
        GetHandleFromBuf(&buffer, &footer.meta_index_handle.offset, &footer.meta_index_handle.size);
        GetHandleFromBuf(&buffer, &footer.index_handle.offset, &footer.index_handle.size);
    }
};


#endif