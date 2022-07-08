#include <Disks/ObjectStorages/StoredObject.h>

StoredObject::StoredObject(
    const std::string & root_path_,
    const std::string & relative_path_,
    uint64_t bytes_size_,
    CacheHintCreator && cache_hint_creator_)
    : bytes_size(bytes_size_)
    , root_path(root_path_)
    , relative_path(relative_path_)
    , cache_hint_creator(std::move(cache_hint_creator_))
{
}

const std::string & StoredObject::getRelativePath() const
{
    return relative_path;
}

std::string StoredObject::getFullPath() const
{
    return std::filesystem::path(root_path) / relative_path;
}

std::string StoredObject::getCacheHint() const
{
    if (!cache_hint_creator)
        return "";

    return cache_hint_creator(getFullPath());
}
