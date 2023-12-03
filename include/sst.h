#include "format.h"
#include <filesystem>

#include <iostream>


struct SST
{
  int fd, file_size;
  char file_name[256];

  std::vector<DataBlock> data_block;
  IndexBlock index_block;
  DataBlock properties;
  MetaindexBlock metaindex_block;
  Footer footer;

  // BlockHandle
  BlockHandle index_block_handle, properties_block_handle, metaindex_block_handle;

  // properties variables
  uint64_t data_blocks, entried, deletions, merge_oprands, range_deletions, raw_key_size, raw_value_size, data_block_size, index_block_size, filter_block_size, entries_for_filter, table_size;

  SST() {}
  SST(char *file_name)
  {
    strncpy(this->file_name, file_name, 256);
    fd = open(file_name, O_RDONLY);
    file_size = std::filesystem::file_size(file_name);

    // init properties variables
    data_blocks = entried = deletions = merge_oprands = range_deletions = raw_key_size = raw_value_size = data_block_size = index_block_size = filter_block_size = entries_for_filter = table_size = 0;
  }

  void insert(Slice key, Slice value)
  {
    if (data_block.size() == 0 || (data_block.back().estimated_size() + key.size + value.size > 4096 && data_block.back().entry_size > 0))
    {
      data_block.push_back(DataBlock());
    }
    data_block.back().insert(std::move(key), std::move(value));
  }

  void decode()
  {
    Buf buf;
    footer.get(fd, file_size, buf);
    // metaindex is not index
    // s | u | v | key | value(BlockHandle)
    metaindex_block.get(fd, footer.metaindex_handle, buf);
    for (auto &kv : metaindex_block.kv)
    {
      if (kv.first.toString() == "rocksdb.properties")
      {
        // values in propertiec block varies from its key
        properties.get(fd, kv.second, buf);
        break;
      }
    }
    index_block.get(fd, footer.index_handle, buf);
    data_block.resize(index_block.kv.size());
    for (int i = 0; i < index_block.kv.size(); i++)
    {
      data_block[i].get(fd, index_block.kv[i].second, buf);
    }
  }

  void encode(int newfd)
  {
    uint64_t offset = 0;
    uint64_t current_data_block_offset, current_data_block_size;
    for (int i = 0; i < data_block.size(); i++)
    {
      current_data_block_offset = offset;
      offset = data_block[i].put_to_fd(newfd, offset);

      current_data_block_size = offset - current_data_block_offset - 5;
      data_block_size += current_data_block_size;
      index_block.put_a_kv(std::move(data_block[i].kv.back().first), BlockHandle(current_data_block_offset, current_data_block_size));
      data_blocks++;
      entried += data_block[i].entry_size;
      raw_key_size += data_block[i].key_size;
      raw_value_size += data_block[i].value_size;
    }

    index_block_handle.offset = offset;
    offset = index_block.put_to_fd(newfd, offset);
    index_block_handle.size = offset - index_block_handle.offset - 5;

    initProperties();
    properties_block_handle.offset = offset;
    offset = properties.put_to_fd(newfd, offset);
    properties_block_handle.size = offset - properties_block_handle.offset - 5;

    metaindex_block_handle.offset = offset;
    initMetaindexBlock();
    offset = metaindex_block.put_to_fd(newfd, offset);
    metaindex_block_handle.size = offset - metaindex_block_handle.offset - 5;

    footer.index_handle.size = index_block_handle.size;
    footer.index_handle.offset = index_block_handle.offset;
    footer.metaindex_handle.size = metaindex_block_handle.size;
    footer.metaindex_handle.offset = metaindex_block_handle.offset;

    offset = footer.put_to_fd(newfd, offset);
  }

  void initMetaindexBlock()
  {
    metaindex_block.kv.clear();
    metaindex_block.insert(Slice("rocksdb.properties"), properties_block_handle);
  }

  // ugly
  void initProperties()
  {
    properties.kv.clear();

    properties.insert(Slice("rocksdb.block.based.table.index.type"), Slice("rocksdb.block.based.table.prefix.filtering"));
    properties.insert(Slice("rocksdb.block.based.table.prefix.filtering"), Slice("0"));
    properties.insert(Slice("rocksdb.block.based.table.whole.key.filtering"), Slice("1"));
    properties.insert(Slice("rocksdb.column.family.id"), Slice("0"));
    properties.insert(Slice("rocksdb.comparator"), Slice("leveldb.BytewiseComparator"));
    properties.insert(Slice("rocksdb.compression"), Slice("NoCompression"));
    properties.insert(Slice("rocksdb.compression_options"), Slice("window_bits=-14; level=32767; strategy=0; max_dict_bytes=0; zstd_max_train_bytes=0; enabled=0; max_dict_buffer_bytes=0; use_zstd_dict_trainer=1;"));
    properties.insert(Slice("rocksdb.creating.db.identity"), Slice("SST Writer"));
    properties.insert(Slice("rocksdb.creating.host.identity"), Slice("DESKTOP-I2II89E"));
    properties.insert(Slice("rocksdb.creating.session.identity"), Slice("W0OUUKKECST2N8Y4YLHL"));
    properties.insert(Slice("rocksdb.creation.time"), Slice("0"));
    properties.insert(Slice("rocksdb.data.size"), Slice(data_block_size));
    properties.insert(Slice("rocksdb.deleted.keys"), Slice(deletions));
    properties.insert(Slice("rocksdb.external_sst_file.global_seqno"), Slice("0"));
    properties.insert(Slice("rocksdb.external_sst_file.version"), Slice("0"));
    properties.insert(Slice("rocksdb.filter.size"), Slice(uint64_t(0)));
    properties.insert(Slice("rocksdb.fixed.key.length"), Slice(uint64_t(0)));
    properties.insert(Slice("rocksdb.format.version"), Slice(uint64_t(5)));
    properties.insert(Slice("rocksdb.index.key.is.user.key"), Slice("0"));
    properties.insert(Slice("rocksdb.index.size"), Slice(index_block_size));
    properties.insert(Slice("rocksdb.index.value.is.delta.encoded"), Slice("0"));
    properties.insert(Slice("rocksdb.merge.operands"), Slice(merge_oprands));
    properties.insert(Slice("rocksdb.merge.operator"), Slice("nullptr"));
    properties.insert(Slice("rocksdb.num.data.blocks"), Slice(data_blocks));
    properties.insert(Slice("rocksdb.num.entries"), Slice(entried));
    properties.insert(Slice("rocksdb.num.filter_entries"), Slice("0"));
    properties.insert(Slice("rocksdb.num.range-deletions"), Slice("0"));
    properties.insert(Slice("rocksdb.oldest.key.time"), Slice("0"));
    properties.insert(Slice("rocksdb.original.file.number"), Slice("0"));
    properties.insert(Slice("rocksdb.prefix.extractor.name"), Slice("nullptr"));
    properties.insert(Slice("rocksdb.property.collectors"), Slice("[]"));
    properties.insert(Slice("rocksdb.raw.key.size"), Slice(raw_key_size));
    properties.insert(Slice("rocksdb.raw.value.size"), Slice(raw_value_size));
    properties.insert(Slice("rocksdb.tail.start.offset"), Slice(uint64_t(0)));
  }
  const char *property_names[34] = {"rocksdb.block.based.table.index.type", "rocksdb.block.based.table.prefix.filtering", "rocksdb.block.based.table.whole.key.filtering", "rocksdb.column.family.id", "rocksdb.comparator", "rocksdb.compression", "rocksdb.compression_options", "rocksdb.creating.db.identity", "rocksdb.creating.host.identity", "rocksdb.creating.session.identity", "rocksdb.creation.time", "rocksdb.data.size", "rocksdb.deleted.keys", "rocksdb.external_sst_file.global_seqno", "rocksdb.external_sst_file.version", "rocksdb.filter.size", "rocksdb.fixed.key.length", "rocksdb.format.version", "rocksdb.index.key.is.user.key", "rocksdb.index.size", "rocksdb.index.value.is.delta.encoded", "rocksdb.merge.operands", "rocksdb.merge.operator", "rocksdb.num.data.blocks", "rocksdb.num.entries", "rocksdb.num.filter_entries", "rocksdb.num.range-deletions", "rocksdb.oldest.key.time", "rocksdb.original.file.number", "rocksdb.prefix.extractor.name", "rocksdb.property.collectors", "rocksdb.raw.key.size", "rocksdb.raw.value.size", "rocksdb.tail.start.offset"};
};
