#pragma once

#if !defined(ARCADIA_BUILD)
#include <Common/config.h>
#endif

#if USE_AWS_S3

#include "RemoteFSMetadata.h"


namespace DB
{
class IDisk;
using ConstDiskPtr = std::shared_ptr<const IDisk>;


struct S3Metadata : VFSMetadataOnDisk
{
    S3Metadata(
        const ConstDiskPtr & disk_,
        const String & remote_fs_root_path_,
        const String & file_path_,
        const String & metadata_path_);

    std::unique_ptr<ReadBuffer> createMetadataReader() const override;

    std::unique_ptr<WriteBuffer> createMetadataWriter() const override;

    bool exists() const override;

    bool isFile() const override;

    void moveFile(const String & to_path) override;

    bool isDirectory() const override;

    void createDirectory() override;

    void createDirectories() override;

    void removeDirectory() override;

    void setLastModified(const Poco::Timestamp & timestamp) override;

    Poco::Timestamp getLastModified() const override;

    void createHardLink(const String & dst_path) override;

private:
    ConstDiskPtr disk;
};

}

#endif
