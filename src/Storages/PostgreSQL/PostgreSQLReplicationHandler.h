#pragma once

#include <common/logger_useful.h>
#include <Storages/StoragePostgreSQL.h>
#include "pqxx/pqxx"


/* Implementation of logical streaming replication protocol: https://www.postgresql.org/docs/10/protocol-logical-replication.html.
 */

namespace DB
{

struct LSNPosition
{
    std::string lsn;

    uint64_t getValue()
    {
        uint64_t upper_half, lower_half, result;
        std::sscanf(lsn.data(), "%lX/%lX", &upper_half, &lower_half);
        result = (upper_half << 32) + lower_half;
        LOG_DEBUG(&Poco::Logger::get("LSNParsing"),
                "Created replication slot. upper half: {}, lower_half: {}, start lsn: {}",
                upper_half, lower_half, result);
        return result;
    }
};


class PostgreSQLReplicationHandler
{
public:
    friend class PGReplicaLSN;
    PostgreSQLReplicationHandler(
            const std::string & database_name_,
            const std::string & table_name_,
            const std::string & conn_str_,
            const std::string & replication_slot_name_,
            const std::string & publication_name_);

    void startup();
    void dropReplicationSlot();

private:
    using NontransactionPtr = std::shared_ptr<pqxx::nontransaction>;

    bool isPublicationExist();
    void createPublication();

    void startReplication();

    bool isReplicationSlotExist(NontransactionPtr ntx);
    void createReplicationSlot(NontransactionPtr ntx);

    void checkConfiguration();

    Poco::Logger * log;
    const std::string database_name, table_name;
    std::string replication_slot_name, publication_name, snapshot_name;

    PostgreSQLConnectionPtr connection;
    PostgreSQLConnectionPtr replication_connection;
    std::shared_ptr<pqxx::work> tx;

    LSNPosition start_lsn, final_lsn;
};


}

