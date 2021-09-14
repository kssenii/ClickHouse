#pragma once

#if !defined(ARCADIA_BUILD)
#include <Common/config.h>
#endif

#include <Common/MultiVersion.h>
#include <Common/ThreadPool.h>
#include <Disks/DiskFactory.h>
#include <Disks/Executor.h>
#include <Disks/RemoteMetadata/RemoteFSMetadata.h>

#include <atomic>
#include <utility>
#include <filesystem>


namespace fs = std::filesystem;

namespace DB
{

/// Helper class to collect paths into chunks of maximum size.
/// For s3 it is Aws::vector<ObjectIdentifier>, for hdfs it is std::vector<std::string>.
class RemoteFSPathKeeper
{
public:
    explicit RemoteFSPathKeeper(size_t chunk_limit_) : chunk_limit(chunk_limit_) {}

    virtual ~RemoteFSPathKeeper() = default;

    virtual void addPath(const String & path) = 0;

protected:
    size_t chunk_limit;
};

using RemoteFSPathKeeperPtr = std::shared_ptr<RemoteFSPathKeeper>;

struct ReservationData
{
    UInt64 reserved_bytes = 0;
    UInt64 reservation_count = 0;
    std::mutex reservation_mutex;
};

/// Base Disk class for remote FS's, which are not posix-compatible (DiskS3 and DiskHDFS)
template <typename Metadata>
class IDiskRemote : public IDisk
{

friend class DiskRemoteReservation;

public:
    IDiskRemote(
        const String & name_,
        const String & remote_fs_root_path_,
        const String & metadata_path_,
        const String & log_name_,
        size_t thread_pool_size);

    const String & getName() const final override { return name; }

    const String & getPath() const final override { return metadata_path; }

    VFSMetadataOnDiskPtr readMeta(const String & path) const;

    virtual VFSMetadataOnDiskPtr createMeta(const String & path) const = 0;

    VFSMetadataOnDiskPtr readOrCreateMetaForWriting(const String & path, WriteMode mode);

    UInt64 getTotalSpace() const override { return std::numeric_limits<UInt64>::max(); }

    UInt64 getAvailableSpace() const override { return std::numeric_limits<UInt64>::max(); }

    UInt64 getUnreservedSpace() const override { return std::numeric_limits<UInt64>::max(); }

    UInt64 getKeepingFreeSpace() const override { return 0; }

    bool exists(const String & path) const override;

    bool isFile(const String & path) const override;

    void createFile(const String & path) override;

    size_t getFileSize(const String & path) const override;

    void moveFile(const String & from_path, const String & to_path) override;

    void replaceFile(const String & from_path, const String & to_path) override;

    void removeFile(const String & path) override { removeSharedFile(path, false); }

    void removeFileIfExists(const String & path) override { removeSharedFileIfExists(path, false); }

    void removeRecursive(const String & path) override { removeSharedRecursive(path, false); }

    void removeSharedFile(const String & path, bool keep_in_remote_fs) override;

    void removeSharedFileIfExists(const String & path, bool keep_in_remote_fs) override;

    void removeSharedRecursive(const String & path, bool keep_in_remote_fs) override;

    void listFiles(const String & path, std::vector<String> & file_names) override;

    void setReadOnly(const String & path) override;

    bool isDirectory(const String & path) const override;

    void createDirectory(const String & path) override;

    void createDirectories(const String & path) override;

    void clearDirectory(const String & path) override;

    void moveDirectory(const String & from_path, const String & to_path) override { moveFile(from_path, to_path); }

    void removeDirectory(const String & path) override;

    DiskDirectoryIteratorPtr iterateDirectory(const String & path) override;

    void setLastModified(const String & path, const Poco::Timestamp & timestamp) override;

    Poco::Timestamp getLastModified(const String & path) override;

    void createHardLink(const String & src_path, const String & dst_path) override;

    ReservationPtr reserve(UInt64 bytes) override;

    String getUniqueId(const String & path) const override;

    bool checkUniqueId(const String & id) const override = 0;

    virtual void removeFromRemoteFS(RemoteFSPathKeeperPtr fs_paths_keeper) = 0;

    virtual RemoteFSPathKeeperPtr createFSPathKeeper() const = 0;

protected:
    Poco::Logger * log;
    const String remote_fs_root_path;
    const String name;
    const String metadata_path;

private:
    void removeMeta(const String & path, RemoteFSPathKeeperPtr fs_paths_keeper);

    void removeMetaRecursive(const String & path, RemoteFSPathKeeperPtr fs_paths_keeper);

    bool tryReserve(UInt64 bytes);

    ReservationData reservation_data;
};

template <typename Metadata> using RemoteDiskPtr = std::shared_ptr<IDiskRemote<Metadata>>;

class RemoteDiskDirectoryIterator final : public IDiskDirectoryIterator
{
public:
    RemoteDiskDirectoryIterator() = default;
    RemoteDiskDirectoryIterator(const String & full_path, const String & folder_path_) : iter(full_path), folder_path(folder_path_) {}

    void next() override { ++iter; }

    bool isValid() const override { return iter != fs::directory_iterator(); }

    String path() const override
    {
        if (fs::is_directory(iter->path()))
            return folder_path / iter->path().filename().string() / "";
        else
            return folder_path / iter->path().filename().string();
    }

    String name() const override { return iter->path().filename(); }

private:
    fs::directory_iterator iter;
    fs::path folder_path;
};


class DiskRemoteReservation final : public IReservation
{
public:
    DiskRemoteReservation(const DiskPtr & disk_, UInt64 size_, ReservationData & data_, Poco::Logger * log_)
        : disk(disk_)
        , size(size_)
        , metric_increment(CurrentMetrics::DiskSpaceReservedForMerge, size_)
        , data(data_)
        , log(log_)
    {
    }

    UInt64 getSize() const override { return size; }

    DiskPtr getDisk(size_t i) const override;

    Disks getDisks() const override { return {disk}; }

    void update(UInt64 new_size) override;

    ~DiskRemoteReservation() override;

private:
    DiskPtr disk;
    UInt64 size;
    CurrentMetrics::Increment metric_increment;
    ReservationData & data;
    Poco::Logger * log;
};


/// Runs tasks asynchronously using thread pool.
class AsyncExecutor : public Executor
{
public:
    explicit AsyncExecutor(const String & name_, int thread_pool_size)
        : name(name_)
        , pool(ThreadPool(thread_pool_size)) {}

    std::future<void> execute(std::function<void()> task) override
    {
        auto promise = std::make_shared<std::promise<void>>();
        pool.scheduleOrThrowOnError(
            [promise, task]()
            {
                try
                {
                    task();
                    promise->set_value();
                }
                catch (...)
                {
                    tryLogCurrentException("Failed to run async task");

                    try
                    {
                        promise->set_exception(std::current_exception());
                    }
                    catch (...) {}
                }
            });

        return promise->get_future();
    }

    void setMaxThreads(size_t threads)
    {
        pool.setMaxThreads(threads);
    }

private:
    String name;
    ThreadPool pool;
};

}
