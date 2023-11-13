import xxhash

def calculate_checksum(file_path, offset, size):
    with open(file_path, 'rb') as f:
        f.seek(offset)
        data = f.read(size)
        # compute XXH3 checksum of data, return in hex format
        checksum = xxhash.xxh3_64(data).hexdigest()
        return checksum


print(calculate_checksum('full.sst', 3326004, 115))
# ff00779f238a501d
# 00 1D 50 8A 23