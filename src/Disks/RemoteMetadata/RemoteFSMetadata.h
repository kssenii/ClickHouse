#pragma once

#if !defined(ARCADIA_BUILD)
#include <Common/config.h>
#endif

#include <Core/Types.h>
#include <Poco/Timestamp.h>

#if USE_AWS_S3
#include <aws/s3/S3Client.h>
#endif


namespace DB
{

class ReadBuffer;
class WriteBuffer;

/** Minimum info, required to be passed to ReadIndirectBufferFromRemoteFS<T>.
 *  Used for Disk types: s3, hdfs, web.
 */
struct VFSMetadata
{
    using PathAndSize = std::pair<String, size_t>;

    std::vector<PathAndSize> remote_fs_objects; /// Remote FS objects paths and their sizes.

    const String remote_fs_root_path;

    const String file_path;

    const String metadata_path;

    VFSMetadata(
        const String & remote_fs_root_path_,
        const String & file_path_,
        const String & metadata_path_);

    String getFullFilePath() const;

    String getFullFilePath(const String & path) const;
};

using VFSMetadataPtr = std::shared_ptr<VFSMetadata>;


/**
 * Remote FS (S3, HDFS) metadata file layout:
 * FS objects, their number and total size of all FS objects.
 */
struct VFSMetadataOnDisk : VFSMetadata
{
    /// Metadata file version.
    static constexpr UInt32 VERSION_ABSOLUTE_PATHS = 1;
    static constexpr UInt32 VERSION_RELATIVE_PATHS = 2;
    static constexpr UInt32 VERSION_READ_ONLY_FLAG = 3;

    /// Total size of all remote FS (S3, HDFS) objects.
    size_t total_size = 0;

    /// Number of references (hardlinks) to this metadata file.
    UInt32 ref_count = 0;

    /// Flag indicates that file is read only.
    bool read_only = false;

    /// Load metadata by path or create empty if `create` flag is set.
    VFSMetadataOnDisk(const String & remote_fs_root_path_,
                     const String & file_path_,
                     const String & metadata_path_);

    virtual ~VFSMetadataOnDisk() = default;

    void read();

    void addObject(const String & path, size_t size);

    /// Fsync metadata file if 'sync' flag is set.
    void save(bool sync = false);

    virtual std::unique_ptr<ReadBuffer> createMetadataReader() const = 0;

    virtual std::unique_ptr<WriteBuffer> createMetadataWriter() const = 0;

    virtual bool exists() const = 0;

    virtual bool isFile() const = 0;

    virtual void moveFile(const String & to_path) = 0;

    virtual bool isDirectory() const = 0;

    virtual void createDirectory() = 0;

    virtual void createDirectories() = 0;

    virtual void removeDirectory() = 0;

    virtual void setLastModified(const Poco::Timestamp & timestamp) = 0;

    virtual Poco::Timestamp getLastModified() const = 0;

    virtual void createHardLink(const String & dst_path) = 0;
};

using VFSMetadataOnDiskPtr = std::shared_ptr<VFSMetadataOnDisk>;

}
