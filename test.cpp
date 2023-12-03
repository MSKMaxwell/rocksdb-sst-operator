#include <stdio.h>
#include <cstdint>
#include "coding.h"
#include <iostream>

using namespace std;
int main()
{
    uint8_t data[5] = {
        0x80, 0xCA, 0x0A, 0xD5, 0x0A};
    char *p = (char *)data, *limit = (char *)data + sizeof(data);
    while (p < limit)
    {
        uint64_t temp;
        p = GetVarint64Ptr(p, limit, &temp);
        cout << temp << endl;
    }
    char t = 0x3a;
    cout << t << endl;
    return 0;
}
/*
-exec x/4xb buf1
0x7fffffffdf60:	0x3e	0x00	0x7a	0x00
-exec x/4xb buf2
0x7fffffffdf64:	0x00	0x7a	0x00	0x3e
*/