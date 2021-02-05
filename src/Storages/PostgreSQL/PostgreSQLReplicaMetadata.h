#pragma once
#include <Interpreters/Context.h>


namespace DB
{

class PostgreSQLReplicaMetadata
{
public:
    PostgreSQLReplicaMetadata(const std::string & metadata_file_path);

    void commitMetadata(std::string & lsn, const std::function<void()> & syncTableFunc);
    void readMetadata();

    size_t version()
    {
        return last_version++;
    }

private:
    void writeMetadata();

    const std::string metadata_file;
    const std::string tmp_metadata_file;

    uint64_t last_version;
    std::string last_lsn;
};

}
