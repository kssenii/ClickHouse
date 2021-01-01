#pragma once

#include <common/logger_useful.h>
#include <Storages/StoragePostgreSQL.h>
#include "pqxx/pqxx"

namespace DB
{

class PGReplicaLSN
{
public:
    PGReplicaLSN() : lsn("") {}
    PGReplicaLSN(const std::string & lsn_str) : lsn(lsn_str) {}
    //PGReplicaLSN(const PGReplicaLSN & lsn_) : lsn(lsn_.getLSNString()) {}

    std::string getLSNString() const { return lsn; }
    uint64_t getLSNNumber() { return parseLSN(); }

private:
    uint64_t parseLSN();
    std::string lsn;
};

class PostgreSQLReplicationHandler
{
public:
    PostgreSQLReplicationHandler(
            const std::string & conn_str_,
            const std::string & replication_slot_name_,
            const std::string & publication_name_);
private:
    void checkConfiguration();
    void startReplication();
    void createReplicationSlot();

    Poco::Logger * log;
    const std::string replication_slot_name;
    const std::string publication_name;

    PGConnectionPtr connection;
    PGConnectionPtr replication_connection;

    std::string temp_slot_name;
    PGReplicaLSN start_lsn, final_lsn;
};



}

