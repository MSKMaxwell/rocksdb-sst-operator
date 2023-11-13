#include "format.h"
#include <filesystem>

struct SST
{
    int fd, file_size;
    char file_name[256];
    Footer footer;
    IndexBlock index_block;
    std::unique_ptr<DataBlock[]> data_block;
    SST(){}
    SST(char *file_name)
    {
        strncpy(this->file_name, file_name, 256);
        fd = open(file_name, O_RDONLY);
        file_size = std::filesystem::file_size(file_name);
    }
    void decode()
    {
        Buf buf;
        footer.get(fd, file_size, buf);
        index_block.get(fd, footer.index_handle, buf);
        data_block = std::make_unique<DataBlock[]>(index_block.handles.size());
        for (int i = 0; i < index_block.handles.size(); i++)
        {
            data_block[i].get(fd, index_block.handles[i], buf);
        }
    }
    void encode()
    {
        // TODO
    }
};
