#include "RemoteFSMetadata.h"

#include <IO/WriteBufferFromFile.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <filesystem>

#if USE_AWS_S3
#include <IO/ReadBufferFromS3.h>
#include <IO/WriteBufferFromS3.h>
#endif

namespace fs = std::filesystem;

namespace DB
{

static constexpr auto MAX_READ_METADATA_TRIES = 10;
static constexpr auto METADATA_BUF_SIZE = 1024;

namespace ErrorCodes
{
    extern const int UNKNOWN_FORMAT;
}

IRemoteFSMetadata::IRemoteFSMetadata(
    const String & remote_fs_root_path_, const String & metadata_file_path_, const String & full_metadata_file_path_)
    : remote_fs_root_path(remote_fs_root_path_)
    , metadata_file_path(metadata_file_path_)
    , full_metadata_file_path(full_metadata_file_path_)
{
}


RemoteFSMetadata::RemoteFSMetadata(
        const String & remote_fs_root_path_,
        const String & metadata_file_path_,
        const String & full_metadata_file_path_)
    : IRemoteFSMetadata(remote_fs_root_path_, metadata_file_path_, full_metadata_file_path_)
    , total_size(0), ref_count(0)
{
}


void RemoteFSMetadata::read()
{
    try
    {
        auto buf = createMetadataReader();

        UInt32 version;
        readIntText(version, *buf);

        if (version < VERSION_ABSOLUTE_PATHS || version > VERSION_READ_ONLY_FLAG)
            throw Exception(
                ErrorCodes::UNKNOWN_FORMAT,
                "Unknown metadata file version. Path: {}. Version: {}. Maximum expected version: {}",
                full_metadata_file_path, toString(version), toString(VERSION_READ_ONLY_FLAG));

        assertChar('\n', *buf);

        UInt32 remote_fs_objects_count;
        readIntText(remote_fs_objects_count, *buf);
        assertChar('\t', *buf);
        readIntText(total_size, *buf);
        assertChar('\n', *buf);
        remote_fs_objects.resize(remote_fs_objects_count);

        for (size_t i = 0; i < remote_fs_objects_count; ++i)
        {
            String remote_fs_object_path;
            size_t remote_fs_object_size;
            readIntText(remote_fs_object_size, *buf);
            assertChar('\t', *buf);
            readEscapedString(remote_fs_object_path, *buf);
            if (version == VERSION_ABSOLUTE_PATHS)
            {
                if (!remote_fs_object_path.starts_with(remote_fs_root_path))
                    throw Exception(ErrorCodes::UNKNOWN_FORMAT,
                        "Path in metadata does not correspond to root path. Path: {}, root path: {}, metadata path: {}",
                        remote_fs_object_path, remote_fs_root_path, full_metadata_file_path);

                remote_fs_object_path = remote_fs_object_path.substr(remote_fs_root_path.size());
            }
            assertChar('\n', *buf);
            remote_fs_objects[i] = {remote_fs_object_path, remote_fs_object_size};
        }

        readIntText(ref_count, *buf);
        assertChar('\n', *buf);

        if (version >= VERSION_READ_ONLY_FLAG)
        {
            readBoolText(read_only, *buf);
            assertChar('\n', *buf);
        }
    }
    catch (Exception & e)
    {
        if (e.code() == ErrorCodes::UNKNOWN_FORMAT)
            throw;

        throw Exception("Failed to read metadata file", e, ErrorCodes::UNKNOWN_FORMAT);
    }
}


void RemoteFSMetadata::addObject(const String & path, size_t size)
{
    total_size += size;
    remote_fs_objects.emplace_back(path, size);
}


/// Fsync metadata file if 'sync' flag is set.
void RemoteFSMetadata::save(bool sync)
{
    auto buf = createMetadataWriter();

    writeIntText(VERSION_RELATIVE_PATHS, *buf);
    writeChar('\n', *buf);

    writeIntText(remote_fs_objects.size(), *buf);
    writeChar('\t', *buf);
    writeIntText(total_size, *buf);
    writeChar('\n', *buf);

    for (const auto & [remote_fs_object_path, remote_fs_object_size] : remote_fs_objects)
    {
        writeIntText(remote_fs_object_size, *buf);
        writeChar('\t', *buf);
        writeEscapedString(remote_fs_object_path, *buf);
        writeChar('\n', *buf);
    }

    writeIntText(ref_count, *buf);
    writeChar('\n', *buf);

    writeBoolText(read_only, *buf);
    writeChar('\n', *buf);

    buf->finalize();
    if (sync)
        buf->sync();
}


LocalMetadata::LocalMetadata(
        const String & remote_fs_root_path_,
        const String & metadata_file_path_,
        const String & disk_path_)
    : RemoteFSMetadata(remote_fs_root_path_, metadata_file_path_, fs::path(disk_path_) / metadata_file_path_)
{
}


std::unique_ptr<ReadBuffer> LocalMetadata::createMetadataReader() const
{
    return std::make_unique<ReadBufferFromFile>(full_metadata_file_path, METADATA_BUF_SIZE);
}


std::unique_ptr<WriteBuffer> LocalMetadata::createMetadataWriter() const
{
    return std::make_unique<WriteBufferFromFile>(full_metadata_file_path, METADATA_BUF_SIZE);
}


#if USE_AWS_S3

S3Metadata::S3Metadata(const String & remote_fs_root_path_,
        const String & metadata_file_path_,
        std::shared_ptr<Aws::S3::S3Client> client_ptr_,
        String bucket_,
        size_t s3_min_upload_part_size_,
        size_t s3_max_single_part_upload_size_)
    : RemoteFSMetadata(remote_fs_root_path_, metadata_file_path_, fs::path(remote_fs_root_path_) / metadata_file_path_)
    , client_ptr(client_ptr_)
    , bucket(bucket_)
    , s3_min_upload_part_size(s3_min_upload_part_size_)
    , s3_max_single_part_upload_size(s3_max_single_part_upload_size_)
{
}


std::unique_ptr<ReadBuffer> S3Metadata::createMetadataReader() const
{
    return std::make_unique<ReadBufferFromS3>(client_ptr, bucket, full_metadata_file_path, MAX_READ_METADATA_TRIES, METADATA_BUF_SIZE);
}


std::unique_ptr<WriteBuffer> S3Metadata::createMetadataWriter() const
{
    return std::make_unique<WriteBufferFromS3>(client_ptr, bucket, full_metadata_file_path, s3_min_upload_part_size, s3_max_single_part_upload_size);
}

#endif

}
