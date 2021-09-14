#include "S3Metadata.h"

#if USE_AWS_S3

#include <IO/ReadBufferFromS3.h>
#include <IO/WriteBufferFromS3.h>
#include <Disks/S3/DiskS3.h>


namespace DB
{

static constexpr auto MAX_READ_METADATA_TRIES = 10;
static constexpr auto METADATA_BUF_SIZE = 1024;


S3Metadata::S3Metadata(
        const ConstDiskPtr & disk_,
        const String & remote_fs_root_path_,
        const String & file_path_,
        const String & metadata_path_)
    : VFSMetadataOnDisk(remote_fs_root_path_, file_path_, metadata_path_)
    , disk(disk_)
{
}


std::unique_ptr<ReadBuffer> S3Metadata::createMetadataReader() const
{
    const auto * s3disk = dynamic_cast<const DiskS3<S3Metadata> *>(disk.get());
    auto settings = s3disk->current_settings.get();
    return std::make_unique<ReadBufferFromS3>(settings->client, s3disk->bucket, getFullFilePath(), MAX_READ_METADATA_TRIES, METADATA_BUF_SIZE);
}


std::unique_ptr<WriteBuffer> S3Metadata::createMetadataWriter() const
{
    const auto * s3disk = dynamic_cast<const DiskS3<S3Metadata> *>(disk.get());
    auto settings = s3disk->current_settings.get();
    return std::make_unique<WriteBufferFromS3>(settings->client, s3disk->bucket, getFullFilePath(), settings->s3_min_upload_part_size, settings->s3_max_single_part_upload_size);
}


bool S3Metadata::exists() const
{
    return true;
}


bool S3Metadata::isFile() const
{
    return true;
}


void S3Metadata::moveFile(const String & /* to_path */)
{
}


void S3Metadata::setLastModified(const Poco::Timestamp &)
{
}


Poco::Timestamp S3Metadata::getLastModified() const
{
    return {};
}


bool S3Metadata::isDirectory() const
{
    return true;
}


void S3Metadata::createDirectory()
{
}


void S3Metadata::createDirectories()
{
}


void S3Metadata::removeDirectory()
{
}


void S3Metadata::createHardLink(const String & /* dst_path */)
{
}

}

#endif
