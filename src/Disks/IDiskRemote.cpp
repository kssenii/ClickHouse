#include <Disks/IDiskRemote.h>

#include "Disks/DiskFactory.h"
#include <IO/ReadBufferFromFile.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/WriteBufferFromS3.h>
#include <IO/WriteHelpers.h>
#include <Common/createHardLink.h>
#include <Common/quoteString.h>
#include <common/logger_useful.h>
#include <Common/checkStackSize.h>
#include <boost/algorithm/string.hpp>
#include <Common/filesystemHelpers.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_DISK_INDEX;
    extern const int UNKNOWN_FORMAT;
    extern const int FILE_ALREADY_EXISTS;
    extern const int PATH_ACCESS_DENIED;;
    extern const int CANNOT_DELETE_DIRECTORY;
}


template <typename Metadata>
MetadataPtr IDiskRemote<Metadata>::readOrCreateMetaForWriting(const String & path, WriteMode mode)
{
    bool exist = exists(path);
    if (exist)
    {
        auto metadata = readMeta(path);
        if (metadata->read_only)
            throw Exception("File is read-only: " + path, ErrorCodes::PATH_ACCESS_DENIED);

        if (mode == WriteMode::Rewrite)
            removeFile(path); /// Remove for re-write.
        else
            return metadata;
    }

    auto metadata = createMeta(path);
    /// Save empty metadata to disk to have ability to get file size while buffer is not finalized.
    metadata->save();

    return metadata;
}


template <typename Metadata>
MetadataPtr IDiskRemote<Metadata>::readMeta(const String & path) const
{
    MetadataPtr metadata = createMeta(path);
    metadata->read();
    return metadata;
}


template <typename Metadata>
MetadataPtr IDiskRemote<Metadata>::createMeta(const String & path) const
{
    if (std::is_same_v<LocalMetadata, Metadata>)
        return std::make_shared<LocalMetadata>(remote_fs_root_path, path, metadata_path);
    else
        return getRemoteMetadata(path);
}


template <typename Metadata>
void IDiskRemote<Metadata>::removeMeta(const String & path, RemoteFSPathKeeperPtr fs_paths_keeper)
{
    LOG_DEBUG(log, "Remove file by path: {}", backQuote(metadata_path + path));

    fs::path file(metadata_path + path);

    if (!fs::is_regular_file(file))
        throw Exception(ErrorCodes::CANNOT_DELETE_DIRECTORY, "Path '{}' is a directory", path);

    try
    {
        auto metadata = readMeta(path);

        /// If there is no references - delete content from remote FS.
        if (metadata->ref_count == 0)
        {
            fs::remove(file);
            for (const auto & [remote_fs_object_path, _] : metadata->remote_fs_objects)
                fs_paths_keeper->addPath(remote_fs_root_path + remote_fs_object_path);
        }
        else /// In other case decrement number of references, save metadata and delete file.
        {
            --metadata->ref_count;
            metadata->save();
            fs::remove(file);
        }
    }
    catch (const Exception & e)
    {
        /// If it's impossible to read meta - just remove it from FS.
        if (e.code() == ErrorCodes::UNKNOWN_FORMAT)
        {
            LOG_WARNING(log,
                "Metadata file {} can't be read by reason: {}. Removing it forcibly.",
                backQuote(path), e.nested() ? e.nested()->message() : e.message());
            fs::remove(file);
        }
        else
            throw;
    }
}


template <typename Metadata>
void IDiskRemote<Metadata>::removeMetaRecursive(const String & path, RemoteFSPathKeeperPtr fs_paths_keeper)
{
    checkStackSize(); /// This is needed to prevent stack overflow in case of cyclic symlinks.

    fs::path file = fs::path(metadata_path) / path;
    if (fs::is_regular_file(file))
    {
        removeMeta(path, fs_paths_keeper);
    }
    else
    {
        for (auto it{iterateDirectory(path)}; it->isValid(); it->next())
            removeMetaRecursive(it->path(), fs_paths_keeper);
        fs::remove(file);
    }
}

DiskPtr DiskRemoteReservation::getDisk(size_t i) const
{
    if (i != 0)
        throw Exception("Can't use i != 0 with single disk reservation", ErrorCodes::INCORRECT_DISK_INDEX);
    return disk;
}


void DiskRemoteReservation::update(UInt64 new_size)
{
    std::lock_guard lock(data.reservation_mutex);
    data.reserved_bytes -= size;
    size = new_size;
    data.reserved_bytes += size;
}


DiskRemoteReservation::~DiskRemoteReservation()
{
    try
    {
        std::lock_guard lock(data.reservation_mutex);
        if (data.reserved_bytes < size)
        {
            data.reserved_bytes = 0;
            /// Note: Disk name is in log (i.e. logger name is DiskS3(disk_name))
            LOG_ERROR(log, "Unbalanced reservations size");
        }
        else
        {
            data.reserved_bytes -= size;
        }

        if (data.reservation_count == 0)
            LOG_ERROR(log, "Unbalanced reservation count");
        else
            --data.reservation_count;
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}


template <typename Metadata>
IDiskRemote<Metadata>::IDiskRemote(
    const String & name_,
    const String & remote_fs_root_path_,
    const String & metadata_path_,
    const String & log_name_,
    size_t thread_pool_size)
    : IDisk(std::make_unique<AsyncExecutor>(log_name_, thread_pool_size))
    , log(&Poco::Logger::get(log_name_ + '(' + getName() + ')'))
    , remote_fs_root_path(remote_fs_root_path_)
    , name(name_)
    , metadata_path(metadata_path_)
{
}


template <typename Metadata>
bool IDiskRemote<Metadata>::exists(const String & path) const
{
    return fs::exists(fs::path(metadata_path) / path);
}


template <typename Metadata>
bool IDiskRemote<Metadata>::isFile(const String & path) const
{
    return fs::is_regular_file(fs::path(metadata_path) / path);
}


template <typename Metadata>
void IDiskRemote<Metadata>::createFile(const String & path)
{
    /// Create empty metadata file.
    auto metadata = createMeta(path);
    metadata->save();
}


template <typename Metadata>
size_t IDiskRemote<Metadata>::getFileSize(const String & path) const
{
    auto metadata = readMeta(path);
    return metadata->total_size;
}


template <typename Metadata>
void IDiskRemote<Metadata>::moveFile(const String & from_path, const String & to_path)
{
    if (exists(to_path))
        throw Exception("File already exists: " + to_path, ErrorCodes::FILE_ALREADY_EXISTS);

    fs::rename(fs::path(metadata_path) / from_path, fs::path(metadata_path) / to_path);
}


template <typename Metadata>
void IDiskRemote<Metadata>::replaceFile(const String & from_path, const String & to_path)
{
    if (exists(to_path))
    {
        const String tmp_path = to_path + ".old";
        moveFile(to_path, tmp_path);
        moveFile(from_path, to_path);
        removeFile(tmp_path);
    }
    else
        moveFile(from_path, to_path);
}


template <typename Metadata>
void IDiskRemote<Metadata>::removeSharedFile(const String & path, bool keep_in_remote_fs)
{
    RemoteFSPathKeeperPtr fs_paths_keeper = createFSPathKeeper();
    removeMeta(path, fs_paths_keeper);
    if (!keep_in_remote_fs)
        removeFromRemoteFS(fs_paths_keeper);
}


template <typename Metadata>
void IDiskRemote<Metadata>::removeSharedFileIfExists(const String & path, bool keep_in_remote_fs)
{
    RemoteFSPathKeeperPtr fs_paths_keeper = createFSPathKeeper();
    if (fs::exists(fs::path(metadata_path) / path))
    {
        removeMeta(path, fs_paths_keeper);
        if (!keep_in_remote_fs)
            removeFromRemoteFS(fs_paths_keeper);
    }
}


template <typename Metadata>
void IDiskRemote<Metadata>::removeSharedRecursive(const String & path, bool keep_in_remote_fs)
{
    RemoteFSPathKeeperPtr fs_paths_keeper = createFSPathKeeper();
    removeMetaRecursive(path, fs_paths_keeper);
    if (!keep_in_remote_fs)
        removeFromRemoteFS(fs_paths_keeper);
}


template <typename Metadata>
void IDiskRemote<Metadata>::setReadOnly(const String & path)
{
    /// We should store read only flag inside metadata file (instead of using FS flag),
    /// because we modify metadata file when create hard-links from it.
    auto metadata = readMeta(path);
    metadata->read_only = true;
    metadata->save();
}


template <typename Metadata>
bool IDiskRemote<Metadata>::isDirectory(const String & path) const
{
    return fs::is_directory(fs::path(metadata_path) / path);
}


template <typename Metadata>
void IDiskRemote<Metadata>::createDirectory(const String & path)
{
    fs::create_directory(fs::path(metadata_path) / path);
}


template <typename Metadata>
void IDiskRemote<Metadata>::createDirectories(const String & path)
{
    fs::create_directories(fs::path(metadata_path) / path);
}


template <typename Metadata>
void IDiskRemote<Metadata>::clearDirectory(const String & path)
{
    for (auto it{iterateDirectory(path)}; it->isValid(); it->next())
        if (isFile(it->path()))
            removeFile(it->path());
}


template <typename Metadata>
void IDiskRemote<Metadata>::removeDirectory(const String & path)
{
    fs::remove(fs::path(metadata_path) / path);
}


template <typename Metadata>
DiskDirectoryIteratorPtr IDiskRemote<Metadata>::iterateDirectory(const String & path)
{
    fs::path meta_path = fs::path(metadata_path) / path;
    if (fs::exists(meta_path) && fs::is_directory(meta_path))
        return std::make_unique<RemoteDiskDirectoryIterator>(meta_path, path);
    else
        return std::make_unique<RemoteDiskDirectoryIterator>();
}


template <typename Metadata>
void IDiskRemote<Metadata>::listFiles(const String & path, std::vector<String> & file_names)
{
    for (auto it = iterateDirectory(path); it->isValid(); it->next())
        file_names.push_back(it->name());
}


template <typename Metadata>
void IDiskRemote<Metadata>::setLastModified(const String & path, const Poco::Timestamp & timestamp)
{
    FS::setModificationTime(fs::path(metadata_path) / path, timestamp.epochTime());
}


template <typename Metadata>
Poco::Timestamp IDiskRemote<Metadata>::getLastModified(const String & path)
{
    return FS::getModificationTimestamp(fs::path(metadata_path) / path);
}


template <typename Metadata>
void IDiskRemote<Metadata>::createHardLink(const String & src_path, const String & dst_path)
{
    /// Increment number of references.
    auto src = readMeta(src_path);
    ++src->ref_count;
    src->save();

    /// Create FS hardlink to metadata file.
    DB::createHardLink(fs::path(metadata_path) / src_path, fs::path(metadata_path) / dst_path);
}


template <typename Metadata>
ReservationPtr IDiskRemote<Metadata>::reserve(UInt64 bytes)
{
    if (!tryReserve(bytes))
        return {};

    return std::make_unique<DiskRemoteReservation>(std::static_pointer_cast<IDiskRemote>(shared_from_this()), bytes, reservation_data, log);
}


template <typename Metadata>
bool IDiskRemote<Metadata>::tryReserve(UInt64 bytes)
{
    std::lock_guard lock(reservation_data.reservation_mutex);
    if (bytes == 0)
    {
        LOG_DEBUG(log, "Reserving 0 bytes on remote_fs disk {}", backQuote(name));
        ++reservation_data.reservation_count;
        return true;
    }

    auto available_space = getAvailableSpace();
    UInt64 unreserved_space = available_space - std::min(available_space, reservation_data.reserved_bytes);
    if (unreserved_space >= bytes)
    {
        LOG_DEBUG(log, "Reserving {} on disk {}, having unreserved {}.",
                  ReadableSize(bytes), backQuote(name), ReadableSize(unreserved_space));
        ++reservation_data.reservation_count;
        reservation_data.reserved_bytes += bytes;
        return true;
    }
    return false;
}

template <typename Metadata>
String IDiskRemote<Metadata>::getUniqueId(const String & path) const
{
    auto metadata = createMeta(path);
    String id;
    if (!metadata->remote_fs_objects.empty())
        id = fs::path(metadata->remote_fs_root_path) / metadata->remote_fs_objects[0].first;
    return id;
}

template
class IDiskRemote<LocalMetadata>;

#if USE_AWS_S3
template
class IDiskRemote<S3Metadata>;
#endif

}
