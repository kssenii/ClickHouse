#include <filesystem>


/// Object metadata: path, size. cache_hint.
struct StoredObject
{
    using CacheHintCreator = std::function<std::string(const std::string &)>;

    explicit StoredObject(
        const std::string & root_path_,
        const std::string & relative_path_,
        uint64_t bytes_size_ = 0,
        CacheHintCreator && cache_hint_creator_ = {});

    const std::string & getRelativePath() const;

    std::string getFullPath() const;

    std::string getCacheHint() const;

    uint64_t bytes_size;

private:
    /// root_path / relative_path == full path
    std::string root_path;
    std::string relative_path;

    /// Optional cache hint for cache. Use delayed initialization
    /// because somecache hint implementation requires it.
    CacheHintCreator cache_hint_creator;
};

using StoredObjects = std::vector<StoredObject>;
