#include "LocalMetadata.h"

#include <Common/filesystemHelpers.h>
#include <Common/createHardLink.h>

#include <IO/WriteBufferFromFile.h>
#include <IO/ReadBufferFromFile.h>

#include <filesystem>


namespace fs = std::filesystem;
namespace DB
{

static constexpr auto METADATA_BUF_SIZE = 1024;

namespace ErrorCodes
{
    extern const int FILE_ALREADY_EXISTS;
    extern const int PATH_ACCESS_DENIED;;
    extern const int CANNOT_DELETE_DIRECTORY;
}


LocalMetadata::LocalMetadata(
        const ConstDiskPtr & /* disk_ */,
        const String & remote_fs_root_path_,
        const String & file_path_,
        const String & metadata_path_)
    : VFSMetadataOnDisk(remote_fs_root_path_, file_path_, metadata_path_)
{
}


std::unique_ptr<ReadBuffer> LocalMetadata::createMetadataReader() const
{
    std::cerr << "\ncreating metadata reader for path: " << getFullFilePath() << std::endl;
    return std::make_unique<ReadBufferFromFile>(getFullFilePath(), METADATA_BUF_SIZE);
}


std::unique_ptr<WriteBuffer> LocalMetadata::createMetadataWriter() const
{
    std::cerr << "\ncreating metadata writer for path: " << getFullFilePath() << std::endl;
    return std::make_unique<WriteBufferFromFile>(getFullFilePath(), METADATA_BUF_SIZE);
}


bool LocalMetadata::exists() const
{
    return fs::exists(getFullFilePath());
}


bool LocalMetadata::isFile() const
{
    return fs::is_regular_file(getFullFilePath());
}


void LocalMetadata::moveFile(const String & to_path)
{
    fs::rename(getFullFilePath(), getFullFilePath(to_path));
}


void LocalMetadata::setLastModified(const Poco::Timestamp & timestamp)
{
    FS::setModificationTime(getFullFilePath(), timestamp.epochTime());
}


Poco::Timestamp LocalMetadata::getLastModified() const
{
    return FS::getModificationTimestamp(getFullFilePath());
}


bool LocalMetadata::isDirectory() const
{
    return fs::is_directory(getFullFilePath());
}


void LocalMetadata::createDirectory()
{
    fs::create_directory(getFullFilePath());
}


void LocalMetadata::createDirectories()
{
    fs::create_directories(getFullFilePath());
}


void LocalMetadata::removeDirectory()
{
    fs::remove(getFullFilePath());
}


void LocalMetadata::createHardLink(const String & dst_path)
{
    DB::createHardLink(getFullFilePath(), getFullFilePath(dst_path));
}

}
