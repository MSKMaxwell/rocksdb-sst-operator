#include <cstdint>

#ifndef _CODING_H_
#define _CODING_H_

#define PERROR(step, err)                \
    {                                    \
        if ((err) == -1)                 \
        {                                \
            perror("error while" #step); \
        }                                \
    }

// copy from rocksdb
// get varint64 from [p], return the next char after varint64
char *GetVarint64Ptr(char *p, char *limit, uint64_t *value)
{
    uint64_t result = 0;
    for (uint32_t shift = 0; shift <= 63 && p < limit; shift += 7)
    {
        uint64_t byte = *(reinterpret_cast<unsigned char *>(p));
        p++;
        if (byte & 128)
        {
            // More bytes are present
            result |= ((byte & 127) << shift);
        }
        else
        {
            result |= (byte << shift);
            *value = result;
            return reinterpret_cast<char *>(p);
        }
    }
    return nullptr;
}

char *EncodeVarint64(char *dst, uint64_t v)
{
    static const unsigned int B = 128;
    unsigned char *ptr = reinterpret_cast<unsigned char *>(dst);
    while (v >= B)
    {
        *(ptr++) = (v & (B - 1)) | B;
        v >>= 7;
    }
    *(ptr++) = static_cast<unsigned char>(v);
    return reinterpret_cast<char *>(ptr);
}

char *EncodeVarint32(char *dst, uint32_t v)
{
    static const unsigned int B = 128;
    unsigned char *ptr = reinterpret_cast<unsigned char *>(dst);
    while (v >= B)
    {
        *(ptr++) = (v & (B - 1)) | B;
        v >>= 7;
    }
    *(ptr++) = static_cast<unsigned char>(v);
    return reinterpret_cast<char *>(ptr);
}

char *GetVarint32Ptr(char *p, char *limit, uint32_t *value)
{
    uint32_t result = 0;
    for (uint32_t shift = 0; shift <= 31 && p < limit; shift += 7)
    {
        uint32_t byte = *(reinterpret_cast<unsigned char *>(p));
        p++;
        if (byte & 128)
        {
            // More bytes are present
            result |= ((byte & 127) << shift);
        }
        else
        {
            result |= (byte << shift);
            *value = result;
            return reinterpret_cast<char *>(p);
        }
    }
    return nullptr;
}

// read [size] bytes into [value] from p, return the next char after [size] bytes
char *GetBytesPtr(char *p, char *limit, char *value, uint32_t size)
{
    for (uint32_t i = 0; i < size && p < limit; i++)
    {
        value[i] = *(reinterpret_cast<unsigned char *>(p));
        p++;
    }
    return reinterpret_cast<char *>(p);
}

char* PutBytesPtr(char* p, char* value, uint32_t size){
    for (uint32_t i = 0; i < size; i++)
    {
        *(reinterpret_cast<unsigned char *>(p)) = value[i];
        p++;
    }
    return reinterpret_cast<char *>(p);
}

uint32_t Lower32of64(uint64_t v) { return static_cast<uint32_t>(v); }

// not used in this code, for debug only

char toHex(unsigned char v)
{
    if (v <= 9)
    {
        return '0' + v;
    }
    return 'A' + v - 10;
}
// most of the code is for validation/error check
int fromHex(char c)
{
    // toupper:
    if (c >= 'a' && c <= 'f')
    {
        c -= ('a' - 'A'); // aka 0x20
    }
    // validation
    if (c < '0' || (c > '9' && (c < 'A' || c > 'F')))
    {
        return -1; // invalid not 0-9A-F hex char
    }
    if (c <= '9')
    {
        return c - '0';
    }
    return c - 'A' + 10;
}

#endif