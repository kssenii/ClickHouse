#include <Storages/StorageMergeTreeParts.h>
#include <Interpreters/Context.h>
#include <QueryPipeline/Pipe.h>
#include <Processors/ISource.h>
#include <Storages/MergeTree/MergeTreeRangeReader.h>
#include <Storages/MergeTree/IMergeTreeReader.h>


namespace DB
{

class StorageMergeTreePartsSource : public ISource
{
public:
    struct PartsToReadInfo
    {
        StorageMergeTreeParts::Parts parts;
        std::atomic<size_t> next_file_to_read = 0;

        std::optional<StorageMergeTreeParts::PartInfo> getNext()
        {
            auto current_part_idx = next_file_to_read.fetch_add(1);
            if (current_part_idx >= parts.size())
                return std::nullopt;
            return parts[current_part_idx];
        }
    };
    using PartsToReadInfoPtr = std::shared_ptr<PartsToReadInfo>;

    StorageMergeTreePartsSource(
        PartsToReadInfoPtr parts_info_,
        const Block & header_)
        : ISource(header_)
        , parts_info(parts_info_)
    {
    }

    String getName() const override
    {
        return "";
    }

    Chunk generate() override
    {
        if (!reader)
        {
            auto part_to_read = parts_info->getNext();
            if (!part_to_read)
                return {};

           //  auto read_info = std::make_shared<MergeTreeDataPartInfoForReader>(
           //      part_to_read->type
           //  );

           //  auto single_disk_volume = std::make_shared<SingleDiskVolume>(disk->getName(), disk, 0);
           //  auto data_part_storage = std::make_shared<DataPartStorageOnDisk>(single_disk_volume, temp_part_dir.parent_path(), part_name);
            // reader = std::make_unique<MergeTreeReader>()
            // range_reader = std::make_unique<MergeTreeRangeReader>(*reader, nullptr, nullptr, true, {});
        }

        // auto read_result = reader->read();

        // if (read_result.num_rows == 0)
        //     return {};

        // return Chunk(std::move(read_result.columns), read_result.num_rows);

        return {};
    }

private:
    PartsToReadInfoPtr parts_info;
    std::unique_ptr<IMergeTreeReader> reader;
    std::unique_ptr<MergeTreeRangeReader> range_reader;
};


StorageMergeTreeParts::StorageMergeTreeParts(
    const Parts & parts_,
    const StorageID & table_id_,
    const ColumnsDescription & columns_,
    const ConstraintsDescription & constraints_,
    ContextPtr context_)
    : IStorage(table_id_)
    , WithContext(context_->getGlobalContext())
    , parts(parts_)
{
    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(columns_);
    storage_metadata.setConstraints(constraints_);
    setInMemoryMetadata(storage_metadata);
}

Pipe StorageMergeTreeParts::read(
    [[maybe_unused]] const Names & column_names,
    [[maybe_unused]] const StorageSnapshotPtr & storage_snapshot,
    [[maybe_unused]] SelectQueryInfo & query_info,
    [[maybe_unused]] ContextPtr context_,
    [[maybe_unused]] QueryProcessingStage::Enum processed_stage,
    [[maybe_unused]] size_t max_block_size,
    unsigned num_streams)
{
    if (num_streams > parts.size())
        num_streams = parts.size();

    Pipes pipes;
    pipes.reserve(num_streams);

    auto parts_info = std::make_shared<StorageMergeTreePartsSource::PartsToReadInfo>();
    Block header;
    for (size_t i = 0; i < num_streams; ++i)
    {
        pipes.emplace_back(
            std::make_shared<StorageMergeTreePartsSource>(parts_info, header));
    }

    return Pipe::unitePipes(std::move(pipes));
}

}
