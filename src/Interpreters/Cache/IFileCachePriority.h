#pragma once

#include <memory>
#include <mutex>
#include <Core/Types.h>
#include <Common/Exception.h>
#include <Interpreters/Cache/FileCacheKey.h>
#include <Interpreters/Cache/Guards.h>
#include <Interpreters/Cache/FileCache_fwd_internal.h>
#include <Common/CurrentMetrics.h>

namespace CurrentMetrics
{
    extern const Metric FilesystemCacheSize;
    extern const Metric FilesystemCacheElements;
}

namespace DB
{
struct LockedFileSegmentMetadata;

/// IFileCachePriority is used to maintain the priority of cached data.
class IFileCachePriority : private boost::noncopyable
{
public:
    using Key = FileCacheKey;
    using KeyAndOffset = FileCacheKeyAndOffset;
    using Entry = FileSegmentMetadataPtr;
    using LockedEntry = LockedFileSegmentMetadata;

    struct Stat
    {
        std::atomic<size_t> current_size = 0;
        std::atomic<size_t> current_elements_num = 0;

        void updateSize(int64_t size)
        {
            chassert(static_cast<int64_t>(current_size) + size >= 0);
            current_size += size;
            CurrentMetrics::add(CurrentMetrics::FilesystemCacheSize, size);
        }

        void updateElements(int64_t num)
        {
            chassert(static_cast<int64_t>(current_elements_num) + num >= 0);
            current_elements_num += num;
            CurrentMetrics::add(CurrentMetrics::FilesystemCacheElements, num);
        }
    };

    /// A priority iterator.
    /// Allows to increase priority of specific entry,
    /// or to remote it.
    class IIterator
    {
    public:
        virtual ~IIterator() = default;

        virtual const Entry & getEntry() const = 0;

        virtual Entry & getEntry() = 0;

        virtual size_t use(const CacheGuard::Lock &) = 0;

        virtual std::shared_ptr<IIterator> remove(const CacheGuard::Lock &) = 0;
    };

    using Iterator = std::shared_ptr<IIterator>;
    using ConstIterator = std::shared_ptr<const IIterator>;

    enum class IterationResult
    {
        BREAK,
        CONTINUE,
        REMOVE_AND_CONTINUE,
    };

    IFileCachePriority(size_t max_size_, size_t max_elements_) : max_size(max_size_), max_elements(max_elements_) {}

    virtual ~IFileCachePriority() = default;

    Stat & getStat() { return stat; }

    size_t getElementsLimit() const { return max_elements; }

    size_t getSizeLimit() const { return max_size; }

    size_t getSize(const CacheGuard::Lock &) const { return stat.current_size; }

    size_t getElementsCount(const CacheGuard::Lock &) const  { return stat.current_elements_num; }

    virtual Iterator add(const Entry & entry, const CacheGuard::Lock &) = 0;

    virtual void pop(const CacheGuard::Lock &) = 0;

    virtual void removeAll(const CacheGuard::Lock &) = 0;

    using IterateFunc = std::function<IterationResult(const LockedEntry &)>;

    /// From lowest to highest priority.
    virtual void iterate(IterateFunc && func, const CacheGuard::Lock &) = 0;

protected:
    const size_t max_size = 0;
    const size_t max_elements = 0;
    Stat stat;
};

}
