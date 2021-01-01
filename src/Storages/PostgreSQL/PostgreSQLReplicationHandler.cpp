#include "PostgreSQLReplicationHandler.h"
#include "PostgreSQLReplicaConsumer.h"

#include <Formats/FormatFactory.h>
#include <Formats/FormatSettings.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

PostgreSQLReplicationHandler::PostgreSQLReplicationHandler(
    const std::string & conn_str,
    const std::string & replication_slot_name_,
    const std::string & publication_name_)
    : log(&Poco::Logger::get("PostgreSQLReplicaHandler"))
    , replication_slot_name(replication_slot_name_)
    , publication_name(publication_name_)
    , connection(std::make_shared<PGConnection>(conn_str))
    , replication_connection(std::make_shared<PGConnection>(fmt::format("{} replication=database", conn_str)))
{
    replication_connection->conn()->set_variable("default_transaction_isolation", "'repeatable read'");
    checkConfiguration();
    startReplication();
}

void PostgreSQLReplicationHandler::checkConfiguration()
{
    auto tx = std::make_unique<pqxx::work>(*connection->conn());
    std::string query_str = fmt::format(
            "(SELECT 'slot_ok' FROM pg_replication_slots WHERE slot_name = '{}') UNION ALL "
            "(SELECT 'publication_ok' FROM pg_publication WHERE pubname = '{}')",
            replication_slot_name, publication_name);

    pqxx::result result{tx->exec(query_str)};

    switch (result.size())
    {
        case 0:
        {
            throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "Invalid configuration: replication_slot_name='{}', publication_name='{}'",
                    replication_slot_name, publication_name);
        }
        case 1:
        {
            pqxx::row row{result[0]};
            if (result[0][0].as<std::string>() == "slot_ok")
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid publication_name='{}'", publication_name);
            else
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid replication_slot_name='{}'", replication_slot_name);
        }
    }

    assert(result.size() == 2);
    tx->commit();
}

void PostgreSQLReplicationHandler::startReplication()
{
    createReplicationSlot();

    PostgreSQLReplicaConsumer consumer(
            replication_connection->conn_str(),
            replication_slot_name,
            publication_name,
            start_lsn);

    consumer.run();
}

// Parse the given XXX/XXX format LSN as reported by postgres,
// into a 64 bit integer as used internally by the wire procotols
uint64_t PGReplicaLSN::parseLSN()
{
    uint64_t upper_half, lower_half, result;
    std::sscanf(lsn.data(), "%lX/%lX", &upper_half, &lower_half);
    result = (upper_half << 32) + lower_half;
    LOG_DEBUG(&Poco::Logger::get("LSNParsing"), "Created replication slot. upper half: {}, lower_half: {}, start lsn: {}",
            upper_half, lower_half, result);
    return result;
}

void PostgreSQLReplicationHandler::createReplicationSlot()
{
    auto tx = std::make_unique<pqxx::work>(*replication_connection->conn());

    std::string query_str = fmt::format(
            "CREATE_REPLICATION_SLOT {} TEMPORARY LOGICAL pgoutput USE_SNAPSHOT",
            "tempslot_kssenii");

    pqxx::result result{tx->exec(query_str)};
    pqxx::row row{result[0]};
    assert(row.size() == 4);

    for (auto res : row)
    {
        if (std::size(res))
            LOG_TRACE(log, "GOT {}", res.as<std::string>());
        else
            LOG_TRACE(log, "GOT NULL");
    }

    temp_slot_name = row[0].as<std::string>();
    start_lsn = PGReplicaLSN(row[1].as<std::string>());

    tx->commit();
    LOG_TRACE(log, "OK");
}

}
