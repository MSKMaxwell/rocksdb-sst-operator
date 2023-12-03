#include "include/sst.h"
#include <filesystem>
#include <iostream>


// use std=c++17

// TODO
// given a fixed-size on-board memory
// but sst file could be pretty large according to its level
// how to take advantage of the memory to encode different parts of a sst file

// input: a kv-pair from hls::stream
// output: encode different parts of a sst file in on-board memory, finally concatenate a sst file
int main()
{
    int fd = open("test_write.sst", O_WRONLY | O_CREAT, 0644);
    SST sst1;
    for(int i = 0; i < 100; i++)
    {
        sst1.insert(Slice(std::to_string(i)), Slice(std::string("test") + std::to_string(i)));
    }
    sst1.encode(fd);
    return 0;
}

