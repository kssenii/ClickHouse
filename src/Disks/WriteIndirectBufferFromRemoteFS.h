#pragma once

#if !defined(ARCADIA_BUILD)
#include <Common/config.h>
#endif

#include <Disks/IDiskRemote.h>
#include <Disks/RemoteMetadata/RemoteFSMetadata.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/WriteBufferFromFileDecorator.h>


namespace DB
{

/// Stores data in S3/HDFS and adds the object path and object size to metadata file on local FS.
template <typename T, typename Metadata>
class WriteIndirectBufferFromRemoteFS final : public WriteBufferFromFileDecorator
{
public:
    WriteIndirectBufferFromRemoteFS(
        std::unique_ptr<T> impl_,
        VFSMetadataOnDiskPtr metadata_,
        const String & remote_fs_path_);

    virtual ~WriteIndirectBufferFromRemoteFS() override;

    void finalize() override;

    void sync() override;

    String getFileName() const override { return metadata->file_path; }

private:
    VFSMetadataOnDiskPtr metadata;

    String remote_fs_path;
};

}
