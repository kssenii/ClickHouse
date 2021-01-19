#pragma once

#include "PostgreSQLConnection.h"
#include "PostgreSQLReplicationHandler.h"
#include "pqxx/pqxx"

namespace DB
{

class PostgreSQLReplicaConsumer
{
public:
    PostgreSQLReplicaConsumer(
            const std::string & table_name_,
            const std::string & conn_str_,
            const std::string & replication_slot_name_,
            const std::string & publication_name_,
            const LSNPosition & start_lsn);

    void run();
    void createSubscription();

private:
    void startReplication(
            const std::string & slot_name, const std::string start_lsn, const int64_t timeline, const std::string & plugin_args);
    void decodeReplicationMessage(const char * replication_message, size_t size);

    Poco::Logger * log;
    const std::string replication_slot_name;
    const std::string publication_name;

    const std::string table_name;
    PostgreSQLConnectionPtr connection, replication_connection;

    LSNPosition current_lsn;
};

}

