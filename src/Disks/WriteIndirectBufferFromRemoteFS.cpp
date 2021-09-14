#include "WriteIndirectBufferFromRemoteFS.h"

#include <IO/WriteBufferFromS3.h>
#include <Storages/HDFS/WriteBufferFromHDFS.h>
#include <Disks/RemoteMetadata/LocalMetadata.h>
#include <Disks/RemoteMetadata/S3Metadata.h>


namespace DB
{

template <typename T, typename Metadata>
WriteIndirectBufferFromRemoteFS<T, Metadata>::WriteIndirectBufferFromRemoteFS(
    std::unique_ptr<T> impl_,
    VFSMetadataOnDiskPtr metadata_,
    const String & remote_fs_path_)
    : WriteBufferFromFileDecorator(std::move(impl_))
    , metadata(metadata_)
    , remote_fs_path(remote_fs_path_)
{
}


template <typename T, typename Metadata>
WriteIndirectBufferFromRemoteFS<T, Metadata>::~WriteIndirectBufferFromRemoteFS()
{
    try
    {
        WriteIndirectBufferFromRemoteFS::finalize();
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}


template <typename T, typename Metadata>
void WriteIndirectBufferFromRemoteFS<T, Metadata>::finalize()
{
    if (finalized)
        return;

    WriteBufferFromFileDecorator::finalize();

    metadata->addObject(remote_fs_path, count());
    metadata->save();
}


template <typename T, typename Metadata>
void WriteIndirectBufferFromRemoteFS<T, Metadata>::sync()
{
    if (finalized)
        metadata->save(true);
}


#if USE_AWS_S3
template
class WriteIndirectBufferFromRemoteFS<WriteBufferFromS3, LocalMetadata>;
template
class WriteIndirectBufferFromRemoteFS<WriteBufferFromS3, S3Metadata>;
#endif

#if USE_HDFS
template
class WriteIndirectBufferFromRemoteFS<WriteBufferFromHDFS, LocalMetadata>;
#endif

}
