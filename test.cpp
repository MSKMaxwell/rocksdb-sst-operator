#include <stdio.h>
#include <cstdint>
#include "coding.h"
#include <iostream>

using namespace std;
int main()
{
    const uint8_t data[5] = {
        0xE4, 0xF8, 0xCA, 0x01, 0x1F};
    char *p = (char *)data, *limit = (char *)data + sizeof(data);
    while (p < limit)
    {
        uint64_t temp;
        p = GetVarint64Ptr(p, limit, &temp);
        cout << temp << endl;
    }
    return 0;
}
/*
-exec x/4xb buf1
0x7fffffffdf60:	0x3e	0x00	0x7a	0x00
-exec x/4xb buf2
0x7fffffffdf64:	0x00	0x7a	0x00	0x3e
*/