#include <Interpreters/Cache/LRUFileCachePriority.h>
#include <Interpreters/Cache/FileCache.h>
#include <Common/CurrentMetrics.h>
#include <Common/randomSeed.h>
#include <Common/logger_useful.h>

namespace CurrentMetrics
{
    extern const Metric FilesystemCacheSize;
    extern const Metric FilesystemCacheElements;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

IFileCachePriority::Iterator LRUFileCachePriority::add(const Entry & entry, const CacheGuard::Lock &)
{
#ifndef NDEBUG
    for (const auto & queue_entry : queue)
    {
        auto locked_entry = entry->lock();
        if (locked_entry->isValid() && entry == queue_entry)
        {
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Attempt to add duplicate queue entry to queue {} (existing entry: {})",
                entry->toString(), queue_entry->toString());
        }
    }
#endif

    const auto & size_limit = getSizeLimit();
    if (size_limit && stat.current_size + entry->size() > size_limit)
    {
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Not enough space to add {}, current size: {}/{}",
            entry->toString(), stat.current_size, size_limit);
    }

    LOG_TEST(log, "Adding entry into LRU queue: {}", entry->toString());
    return std::make_shared<LRUFileCacheIterator>(this, queue.emplace(queue.end(), entry));
}

void LRUFileCachePriority::removeAll(const CacheGuard::Lock &)
{
    LOG_TEST(log, "Removing all entries from LRU queue");
    queue.clear();
}

void LRUFileCachePriority::pop(const CacheGuard::Lock &)
{
    remove(queue.begin());
}

LRUFileCachePriority::LRUQueueIterator LRUFileCachePriority::remove(LRUQueueIterator it)
{
    LOG_TEST(log, "Removing entry from LRU queue: {}", (*it)->toString());
    return queue.erase(it);
}

LRUFileCachePriority::LRUFileCacheIterator::LRUFileCacheIterator(
    LRUFileCachePriority * cache_priority_,
    LRUFileCachePriority::LRUQueueIterator queue_iter_)
    : cache_priority(cache_priority_)
    , queue_iter(queue_iter_)
{
}

void LRUFileCachePriority::iterate(IterateFunc && func, const CacheGuard::Lock &)
{
    for (auto it = queue.begin(); it != queue.end();)
    {
        auto locked_entry = (*it)->lock();
        if (!locked_entry->isValid())
            continue;

        auto result = func(*locked_entry);
        switch (result)
        {
            case IterationResult::BREAK:
            {
                return;
            }
            case IterationResult::CONTINUE:
            {
                ++it;
                break;
            }
            case IterationResult::REMOVE_AND_CONTINUE:
            {
                it = remove(it);
                break;
            }
        }
    }
}

LRUFileCachePriority::Iterator
LRUFileCachePriority::LRUFileCacheIterator::remove(const CacheGuard::Lock &)
{
    return std::make_shared<LRUFileCacheIterator>(
        cache_priority, cache_priority->remove(queue_iter));
}

size_t LRUFileCachePriority::LRUFileCacheIterator::use(const CacheGuard::Lock &)
{
    cache_priority->queue.splice(cache_priority->queue.end(), cache_priority->queue, queue_iter);
    auto & entry = *(++queue_iter);
    return entry->hits;
}

}
