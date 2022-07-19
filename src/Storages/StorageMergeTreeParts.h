#pragma once
#include <Storages/IStorage.h>
#include <Storages/MergeTree/MergeTreeDataPartType.h>

namespace DB
{

class StorageMergeTreeParts final : public IStorage, public WithContext
{
public:
    struct PartInfo
    {
        MergeTreeDataPartType type;
        std::vector<std::string> files;
        std::pair<size_t, size_t> range;
    };
    using Parts = std::vector<PartInfo>;

    StorageMergeTreeParts(
        const Parts & parts_,
        const StorageID & table_id_,
        const ColumnsDescription & columns_,
        const ConstraintsDescription & constraints_,
        ContextPtr context_);

    std::string getName() const override { return "MergeTreeParts"; }

    Pipe read(
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        unsigned num_streams) override;

private:
    Parts parts;
};

}
