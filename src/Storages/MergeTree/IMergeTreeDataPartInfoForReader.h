#pargma once
#include <Interpreters/Context.h>
#include <Storages/MergeTree/AlterConversions.h>

namespace DB
{

class IDataPartStorage;
using DataPartStoragePtr = std::shared_ptr<IDataPartStorage>;
class MergeTreeIndexGranularity;
struct MergeTreeDataPartChecksums;
struct MergeTreeIndexGranularityInfo;


/**
 * A class which contains all information about a data part that is required
 * in order to use MergeTreeDataPartReader's.
 */
class IMergeTreeDataPartInfoForReader : public WithContext
{
public:
    explicit IMergeTreeDataPartInfoForReader(ContextPtr context_) : WithContext(context_) {}

    virtual ~IMergeTreeDataPartInfoForReader() = default;

    virtual bool isCompactPart() const = 0;

    virtual bool isWidePart() const = 0;

    virtual bool isInMemoryPart() const = 0;

    virtual const DataPartStoragePtr & getDataPartStorage() const = 0;

    virtual const NamesAndTypesList & getColumns() const = 0;

    virtual std::optional<size_t> getColumnPosition(const String & column_name) const = 0;

    virtual const MergeTreeDataPartChecksums & getChecksums() const = 0;

    virtual AlterConversions getAlterConversions() const = 0;

    virtual size_t getMarksCount() const = 0;

    virtual size_t getFileSizeOrZero(const std::string & file_name) const = 0;

    virtual const MergeTreeIndexGranularityInfo & getIndexGranularityInfo() const = 0;

    virtual const MergeTreeIndexGranularity & getIndexGranularity() const = 0;

    virtual SerializationPtr getSerialization(const NameAndTypePair & column) const = 0;

    virtual void reportBroken() = 0;
};

using MergeTreeDataPartInfoForReaderPtr = std::shared_ptr<IMergeTreeDataPartInfoForReader>;

}
