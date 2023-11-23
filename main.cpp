#include "include/sst.h"
#include <filesystem>
#include <iostream>


// use std=c++17

int main()
{
    // read a sst and write it back - test passed
    // int fd = open("write_sst_test", O_WRONLY | O_CREAT, 0644);
    // SST sst1("test.sst");
    // sst1.decode();
    // sst1.encode(fd);

    // TODO
    // put in key-value pairs and write it back - test
    int fd = open("write_sst_test", O_WRONLY | O_CREAT, 0644);
    SST sst3;
    for(int i = 0; i < 1000; i++)
    {
        std::string temp("test_data");
        sst3.insert(std::to_string(i), temp + std::to_string(i));
    }
    sst3.encode(fd);

    SST sst2("write_sst_test");
    sst2.decode();
    freopen("output.txt", "w", stdout);
    for (int i = 0; i < sst2.index_block.kv.size(); i++)
    {
        for (int j = 0; j < sst2.data_block[i].kv.size(); j++)
        {
            std::cout << sst2.data_block[i].kv[j].first.toString() << " " << sst2.data_block[i].kv[j].second.toString() << std::endl;
        }
    }
    return 0;
}

