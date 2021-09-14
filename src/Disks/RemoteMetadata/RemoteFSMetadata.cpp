#include "RemoteFSMetadata.h"

#include <Common/createHardLink.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <filesystem>

namespace fs = std::filesystem;

namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_FORMAT;
}

VFSMetadata::VFSMetadata(
    const String & remote_fs_root_path_, const String & file_path_, const String & metadata_path_)
    : remote_fs_root_path(remote_fs_root_path_)
    , file_path(file_path_)
    , metadata_path(metadata_path_)
{
}


String VFSMetadata::getFullFilePath(const String & path) const
{
    return fs::path(metadata_path) / path;
}


String VFSMetadata::getFullFilePath() const
{
    return fs::path(metadata_path) / file_path;
}


VFSMetadataOnDisk::VFSMetadataOnDisk(

        const String & remote_fs_root_path_,
        const String & file_path_,
        const String & metadata_path_)
    : VFSMetadata(remote_fs_root_path_, file_path_, metadata_path_)
    , total_size(0), ref_count(0)
{
}


void VFSMetadataOnDisk::read()
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
                getFullFilePath(file_path), toString(version), toString(VERSION_READ_ONLY_FLAG));

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
                        remote_fs_object_path, remote_fs_root_path, getFullFilePath(file_path));

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


void VFSMetadataOnDisk::addObject(const String & path, size_t size)
{
    total_size += size;
    remote_fs_objects.emplace_back(path, size);
}


/// Fsync metadata file if 'sync' flag is set.
void VFSMetadataOnDisk::save(bool sync)
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

}
