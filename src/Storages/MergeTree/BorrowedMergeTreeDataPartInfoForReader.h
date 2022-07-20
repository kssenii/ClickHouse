#pragma once
#include <Storages/MergeTree/IMergeTreeDataPartInfoForReader.h>
#include <Storages/MergeTree/MergeTreeDataPartType.h>
#include <Storages/MergeTree/AlterConversions.h>
#include <Storages/MergeTree/MergeTreeIndexGranularity.h>
#include <Storages/MergeTree/MergeTreeIndexGranularityInfo.h>


namespace DB
{

class BorrowedMergeTreeDataPartInfoForReader final : public IMergeTreeDataPartInfoForReader
{
public:
    bool isCompactPart() const override { return type == MergeTreeDataPartType::Compact; }

    bool isWidePart() const override { return type == MergeTreeDataPartType::Wide; }

    bool isInMemoryPart() const override { return type == MergeTreeDataPartType::InMemory; }

    const DataPartStoragePtr & getDataPartStorage() const override { return data_part_storage; }

    const NamesAndTypesList & getColumns() const override { return columns; }

    AlterConversions getAlterConversions() const override { return {}; } /// Noop.

    const MergeTreeDataPartChecksums & getChecksums() const override { return checksums; }

    std::optional<size_t> getColumnPosition(const String & column_name) const override
    {
        auto it = column_name_to_position.find(column_name);
        if (it == column_name_to_position.end())
            return {};
        return it->second;
    }

    size_t getMarksCount() const override { return marks_count; }

    size_t getFileSizeOrZero(const std::string & file_name) const override
    {
        auto checksum = checksums.files.find(file_name);
        if (checksum == checksums.files.end())
            return 0;
        return checksum->second.file_size;
    }

    const MergeTreeIndexGranularityInfo & getIndexGranularityInfo() const override { return index_granularity_info; }

    const MergeTreeIndexGranularity & getIndexGranularity() const override { return index_granularity; }

    SerializationPtr getSerialization(const NameAndTypePair & column) const override
    {
        auto it = serialization_infos.find(column.getNameInStorage());
        return it == serialization_infos.end()
            ? IDataType::getSerialization(column)
            : IDataType::getSerialization(column, *it->second);
    }

    void reportBroken() override {} /// Noop.

    BorrowedMergeTreeDataPartInfoForReader(
        MergeTreeDataPartType type_,
        DataPartStoragePtr data_part_storage_,
        NamesAndTypesList columns_,
        MergeTreeIndexGranularityInfo index_granularity_info_,
        MergeTreeIndexGranularity index_granularity_,
        MergeTreeDataPartChecksums checksums_,
        SerializationInfoByName serialization_infos_,
        size_t marks_count_,
        ContextPtr context_)
    : IMergeTreeDataPartInfoForReader(context_)
    , type(type_)
    , data_part_storage(data_part_storage_)
    , columns(columns_)
    , index_granularity_info(index_granularity_info_)
    , index_granularity(index_granularity_)
    , checksums(checksums_)
    , serialization_infos(serialization_infos_)
    , marks_count(marks_count_)
{
}

private:
    MergeTreeDataPartType type;
    DataPartStoragePtr data_part_storage;
    NamesAndTypesList columns;
    MergeTreeIndexGranularityInfo index_granularity_info;
    MergeTreeIndexGranularity index_granularity;
    MergeTreeDataPartChecksums checksums;
    SerializationInfoByName serialization_infos;
    size_t marks_count;
    std::unordered_map<std::string, size_t> column_name_to_position;
};


}
