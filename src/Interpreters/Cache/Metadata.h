#pragma once
#include <boost/noncopyable.hpp>
#include <Interpreters/Cache/Guards.h>
#include <Interpreters/Cache/IFileCachePriority.h>
#include <Interpreters/Cache/FileCacheKey.h>
#include <Interpreters/Cache/FileSegment.h>
#include <Interpreters/Cache/FileCache_fwd_internal.h>

namespace DB
{
class CleanupQueue;
using CleanupQueuePtr = std::shared_ptr<CleanupQueue>;

struct LockedFileSegmentMetadata;
using LockedFileSegmentMetadataPtr = std::unique_ptr<LockedFileSegmentMetadata>;

/// An element in a priority queue.
class FileSegmentMetadata : boost::noncopyable
{
public:
    using Key = FileCacheKey;
    using GlobalStat = IFileCachePriority::Stat;
    using Guard = CacheFileSegmentMetadataGuard;
    friend struct LockedFileSegmentMetadata;
    friend struct EvictionCandidates;

    FileSegmentMetadata(FileSegmentPtr && file_segment_, size_t reserved_size_, GlobalStat & global_stat_);

    ~FileSegmentMetadata();

    const Key & key() const { return file_segment->key(); }

    size_t offset() const { return file_segment->offset(); }

    FileSegment::Range range() const { return file_segment->range(); }

    bool operator == (const FileSegmentMetadata & other) const
    {
        return key() == other.key() && offset() == other.offset();
    }

    LockedFileSegmentMetadataPtr lock();

    std::string toString() const { return fmt::format("{}:{}:{}", key(), offset(), reserved_size); }

    const FileSegment & getFileSegment() const { return *file_segment; }

    FileSegmentPtr getSnapshot() { return FileSegment::getSnapshot(file_segment); }

    void unmarkEvicting();

    size_t hits = 0;

private:
    const FileSegmentPtr file_segment;
    IFileCachePriority::Stat & global_stat;
    Guard guard;

    bool isValid(const Guard::Lock &) const { return is_valid; }
    void invalidate(const Guard::Lock &);
    void updateSize(size_t size, const Guard::Lock &);
    bool releasable(const Guard::Lock &) const { return file_segment.unique(); }
    size_t size(const Guard::Lock &) const { return reserved_size; }
    bool addToList(FileSegments & file_segments, const Guard::Lock &);

    size_t reserved_size = 0;
    bool is_valid = true;
    bool evicting = false;
};

using FileSegmentMetadataPtr = std::shared_ptr<FileSegmentMetadata>;

struct LockedFileSegmentMetadata : boost::noncopyable
{
    explicit LockedFileSegmentMetadata(FileSegmentMetadata & metadata_) : lock(metadata_.guard.lock()), metadata(metadata_) {}

    const FileSegmentMetadata & get() const { return metadata; }

    bool releasable() const { return metadata.releasable(lock); }

    void markEvicting() const
    {
        if (!metadata.releasable(lock))
        {
            throw Exception();
        }
        metadata.evicting = true;
    }
    void unmarkEvicting() const
    {
        metadata.evicting = false;
    }

    void resetEvicting()
    {
        metadata.evicting = false;
    }

    size_t size() const { return metadata.size(lock); }

    bool isValid() const { return metadata.isValid(lock); }

    void invalidate() { metadata.invalidate(lock); }

    void updateSize(int64_t size) { metadata.updateSize(size, lock); }
    bool addToList(FileSegments & file_segments) { return metadata.addToList(file_segments, lock); }

    CacheFileSegmentMetadataGuard::Lock lock;
    FileSegmentMetadata & metadata;
};

struct KeyMetadata : public std::map<size_t, FileSegmentMetadataPtr>,
                     private boost::noncopyable,
                     public std::enable_shared_from_this<KeyMetadata>
{
    friend struct LockedKey;
    using Key = FileCacheKey;

    KeyMetadata(
        const Key & key_,
        const std::string & key_path_,
        CleanupQueue & cleanup_queue_,
        bool created_base_directory_ = false);

    enum class KeyState
    {
        ACTIVE,
        REMOVING,
        REMOVED,
    };

    const Key key;
    const std::string key_path;

    LockedKeyPtr lock();

    /// Return nullptr if key has non-ACTIVE state.
    LockedKeyPtr tryLock();

    bool createBaseDirectory();

    std::string getFileSegmentPath(const FileSegment & file_segment);

private:
    KeyState key_state = KeyState::ACTIVE;
    KeyGuard guard;
    CleanupQueue & cleanup_queue;
    std::atomic<bool> created_base_directory = false;
};

using KeyMetadataPtr = std::shared_ptr<KeyMetadata>;


struct CacheMetadata : public std::unordered_map<FileCacheKey, KeyMetadataPtr>, private boost::noncopyable
{
public:
    using Key = FileCacheKey;
    using IterateCacheMetadataFunc = std::function<void(const LockedKey &)>;

    explicit CacheMetadata(const std::string & path_);

    const String & getBaseDirectory() const { return path; }

    String getPathForFileSegment(
        const Key & key,
        size_t offset,
        FileSegmentKind segment_kind) const;

    String getPathForKey(const Key & key) const;
    static String getFileNameForFileSegment(size_t offset, FileSegmentKind segment_kind);

    void iterate(IterateCacheMetadataFunc && func);

    enum class KeyNotFoundPolicy
    {
        THROW,
        CREATE_EMPTY,
        RETURN_NULL,
    };

    LockedKeyPtr lockKeyMetadata(
        const Key & key,
        KeyNotFoundPolicy key_not_found_policy,
        bool is_initial_load = false);

    void doCleanup();

private:
    CacheMetadataGuard::Lock lockMetadata() const;
    const std::string path; /// Cache base path
    mutable CacheMetadataGuard guard;
    const CleanupQueuePtr cleanup_queue;
    Poco::Logger * log;
};


/**
 * `LockedKey` is an object which makes sure that as long as it exists the following is true:
 * 1. the key cannot be removed from cache
 *    (Why: this LockedKey locks key metadata mutex in ctor, unlocks it in dtor, and so
 *    when key is going to be deleted, key mutex is also locked.
 *    Why it cannot be the other way round? E.g. that ctor of LockedKey locks the key
 *    right after it was deleted? This case it taken into consideration in createLockedKey())
 * 2. the key cannot be modified, e.g. new offsets cannot be added to key; already existing
 *    offsets cannot be deleted from the key
 * And also provides some methods which allow the owner of this LockedKey object to do such
 * modification of the key (adding/deleting offsets) and deleting the key from cache.
 */
struct LockedKey : private boost::noncopyable
{
    using Key = FileCacheKey;

    explicit LockedKey(std::shared_ptr<KeyMetadata> key_metadata_);

    ~LockedKey();

    const Key & getKey() const { return key_metadata->key; }

    auto begin() const { return key_metadata->begin(); }
    auto end() const { return key_metadata->end(); }

    std::shared_ptr<const FileSegmentMetadata> getByOffset(size_t offset) const;
    std::shared_ptr<FileSegmentMetadata> getByOffset(size_t offset);

    std::shared_ptr<const FileSegmentMetadata> tryGetByOffset(size_t offset) const;
    std::shared_ptr<FileSegmentMetadata> tryGetByOffset(size_t offset);

    KeyMetadata::KeyState getKeyState() const { return key_metadata->key_state; }

    std::shared_ptr<const KeyMetadata> getKeyMetadata() const { return key_metadata; }
    std::shared_ptr<KeyMetadata> getKeyMetadata() { return key_metadata; }

    void removeAllReleasable();

    KeyMetadata::iterator removeFileSegment(size_t offset, const FileSegmentGuard::Lock &);

    void shrinkFileSegmentToDownloadedSize(size_t offset, const FileSegmentGuard::Lock &);

    bool isLastOwnerOfFileSegment(size_t offset) const;

    void removeFromCleanupQueue();

    void markAsRemoved();

    std::string toString() const;

private:
    const std::shared_ptr<KeyMetadata> key_metadata;
    KeyGuard::Lock lock; /// `lock` must be destructed before `key_metadata`.
    Poco::Logger * log;
};

}
