#pragma once

#if !defined(ARCADIA_BUILD)
#include <Common/config.h>
#endif

#include <Core/Types.h>

#if USE_AWS_S3
#include <aws/s3/S3Client.h>
#endif


namespace DB
{

class ReadBuffer;
class WriteBuffer;
class ReadBufferFromFile;
class WriteBufferFromFile;

#if USE_AWS_S3
class ReadBufferFromS3;
class WriteBufferFromS3;
#endif

struct LocalMetadataHelpers
{
    using Reader = ReadBufferFromFile;
    using Writer = ReadBufferFromFile;
};

#if USE_AWS_S3
struct S3MetadataHelpers
{
    using Reader = ReadBufferFromS3;
    using Writer = WriteBufferFromS3;
};
#endif

/** Minimum info, required to be passed to ReadIndirectBufferFromRemoteFS<T>.
 *  Used for Disk types: s3, hdfs, web.
 */
struct IRemoteFSMetadata
{
    using PathAndSize = std::pair<String, size_t>;

    /// Remote FS objects paths and their sizes.
    std::vector<PathAndSize> remote_fs_objects;

    /// URI
    const String & remote_fs_root_path;

    /// Relative path to metadata file
    const String metadata_file_path;

    /// Full metadata path. For local metadata it is disk_path / metadata_file_path,
    /// for remote metadata it is url / metadata_file_path.
    const String full_metadata_file_path;

    IRemoteFSMetadata(const String & remote_fs_root_path_,
        const String & metadata_file_path_, const String & full_metadata_path_);
};

using IMetadataPtr = std::shared_ptr<IRemoteFSMetadata>;


/**
 * Remote FS (S3, HDFS) metadata file layout:
 * FS objects, their number and total size of all FS objects.
 */
struct RemoteFSMetadata : IRemoteFSMetadata
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
    RemoteFSMetadata(const String & remote_fs_root_path_,
                     const String & metadata_file_path_,
                     const String & full_metadata_file_path_);

    virtual ~RemoteFSMetadata() = default;

    void read();

    void addObject(const String & path, size_t size);

    /// Fsync metadata file if 'sync' flag is set.
    void save(bool sync = false);

    virtual std::unique_ptr<ReadBuffer> createMetadataReader() const = 0;

    virtual std::unique_ptr<WriteBuffer> createMetadataWriter() const = 0;
};

using MetadataPtr = std::shared_ptr<RemoteFSMetadata>;


struct LocalMetadata : RemoteFSMetadata
{
    LocalMetadata(const String & remote_fs_root_path_,
                  const String & metadata_file_path_,
                  const String & disk_path_);

    std::unique_ptr<ReadBuffer> createMetadataReader() const override;
    std::unique_ptr<WriteBuffer> createMetadataWriter() const override;
};

#if USE_AWS_S3
struct S3Metadata : RemoteFSMetadata
{
    S3Metadata(const String & remote_fs_root_path_,
               const String & metadata_file_path_,
               std::shared_ptr<Aws::S3::S3Client> client_ptr_,
               String bucket_,
               size_t s3_min_upload_part_size_,
               size_t s3_max_single_part_upload_size_);

    std::shared_ptr<Aws::S3::S3Client> client_ptr;
    String bucket;
    size_t s3_min_upload_part_size;
    size_t s3_max_single_part_upload_size;

    std::unique_ptr<ReadBuffer> createMetadataReader() const override;
    std::unique_ptr<WriteBuffer> createMetadataWriter() const override;
};
#endif

}
