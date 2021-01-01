#include "PostgreSQLReplicationHandler.h"
#include "PostgreSQLReplicaConsumer.h"

#include <Formats/FormatFactory.h>
#include <Formats/FormatSettings.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int UNKNOWN_TABLE;
    extern const int LOGICAL_ERROR;
}

PostgreSQLReplicationHandler::PostgreSQLReplicationHandler(
    const std::string & database_name_,
    const std::string & table_name_,
    const std::string & conn_str,
    const std::string & replication_slot_name_,
    const std::string & publication_name_)
    : log(&Poco::Logger::get("PostgreSQLReplicaHandler"))
    , database_name(database_name_)
    , table_name(table_name_)
    , replication_slot_name(replication_slot_name_)
    , publication_name(publication_name_)
    , connection(std::make_shared<PostgreSQLConnection>(conn_str))
{
    /// Passing 'database' as the value instructs walsender to connect to the database specified in the dbname parameter,
    /// which will allow the connection to be used for logical replication from that database.
    replication_connection = std::make_shared<PostgreSQLConnection>(fmt::format("{} replication=database", conn_str));

    /// Replication slots can be created only with a specific transaction isolation type.
    replication_connection->conn()->set_variable("default_transaction_isolation", "'repeatable read'");

    if (replication_slot_name.empty())
        replication_slot_name = fmt::format("{}_{}_ch_replication_slot", database_name, table_name);
}


void PostgreSQLReplicationHandler::startup()
{
    tx = std::make_shared<pqxx::work>(*connection->conn());
    if (publication_name.empty())
    {
        publication_name = fmt::format("{}_{}_ch_publication", database_name, table_name);

        if (!isPublicationExist())
            createPublication();
    }
    else if (!isPublicationExist())
    {
        throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Publication name '{}' is spesified in table arguments, but it does not exist", publication_name);
    }
    tx->commit();

    startReplication();
}


bool PostgreSQLReplicationHandler::isPublicationExist()
{
    std::string query_str = fmt::format("SELECT exists (SELECT 1 FROM pg_publication WHERE pubname = '{}')", publication_name);
    pqxx::result result{tx->exec(query_str)};
    bool publication_exists = (result[0][0].as<std::string>() == "t");

    if (publication_exists)
        LOG_TRACE(log, "Publication {} already exists. Using existing version");

    return publication_exists;
}


void PostgreSQLReplicationHandler::createPublication()
{
    /* * It is also important that change replica identity for this table to be able to receive old values of updated rows:
     *   ALTER TABLE pgbench_accounts REPLICA IDENTITY FULL;
     * * TRUNCATE and DDL are not included in PUBLICATION.
     * * 'ONLY' means just a table, without descendants.
     */
    std::string query_str = fmt::format("CREATE PUBLICATION {} FOR TABLE ONLY {}", publication_name, table_name);
    try
    {
        tx->exec(query_str);
        LOG_TRACE(log, "Created publication {}", publication_name);
    }
    catch (pqxx::undefined_table const &)
    {
        throw Exception(fmt::format(
                    "PostgreSQL table {}.{} does not exist",
                    database_name, table_name), ErrorCodes::UNKNOWN_TABLE);
    }
}


void PostgreSQLReplicationHandler::startReplication()
{
    PostgreSQLReplicaConsumer consumer(
            table_name,
            connection->conn_str(),
            replication_slot_name,
            publication_name,
            start_lsn);

    auto ntx = std::make_shared<pqxx::nontransaction>(*replication_connection->conn());

    if (isReplicationSlotExist(ntx))
        dropReplicationSlot();

    createReplicationSlot(ntx);

    consumer.loadFromSnapshot(snapshot_name);

    LOG_DEBUG(log, "Commiting replication transaction");
    ntx->commit();

    //consumer.run();
}


bool PostgreSQLReplicationHandler::isReplicationSlotExist(NontransactionPtr ntx)
{
    std::string query_str = fmt::format(
            "SELECT active, restart_lsn FROM pg_replication_slots WHERE slot_name = '{}'",
            replication_slot_name);
    pqxx::result result{ntx->exec(query_str)};

    /// Replication slot does not exist
    if (result.empty())
        return false;

    bool is_active = result[0][0].as<bool>();
    start_lsn.lsn = result[0][0].as<std::string>();
    LOG_TRACE(log, "Replication slot {} already exists (active: {}). Restart lsn position is {}",
            replication_slot_name, is_active, start_lsn.lsn);

    return true;
}


void PostgreSQLReplicationHandler::createReplicationSlot(NontransactionPtr ntx)
{
    std::string query_str = fmt::format(
            "CREATE_REPLICATION_SLOT {} TEMPORARY LOGICAL pgoutput EXPORT_SNAPSHOT",
            replication_slot_name);
    try
    {
        pqxx::result result{ntx->exec(query_str)};
        start_lsn.lsn = result[0][1].as<std::string>();
        snapshot_name = result[0][2].as<std::string>();
        LOG_TRACE(log, "Created replication slot with name: {}, start lsn: {}, snapshot name: {}",
                replication_slot_name, start_lsn.lsn, snapshot_name);
    }
    catch (Exception & e)
    {
        e.addMessage("while creating PostgreSQL replication slot");
        throw;
    }
}


void PostgreSQLReplicationHandler::dropReplicationSlot()
{
    if (!start_lsn.lsn.empty())
    {
        pqxx::work work(*connection->conn());
        std::string query_str = fmt::format("SELECT pg_drop_replication_slot('{}')", replication_slot_name);
        work.exec(query_str);
        work.commit();
    }
}


//void PostgreSQLReplicationHandler::checkConfiguration()
//{
//    auto tx = std::make_unique<pqxx::work>(*connection->conn());
//    std::string query_str = fmt::format(
//            "(SELECT 'slot_ok' FROM pg_replication_slots WHERE slot_name = '{}') UNION ALL "
//            "(SELECT 'publication_ok' FROM pg_publication WHERE pubname = '{}')",
//            replication_slot_name, publication_name);
//
//    pqxx::result result{tx->exec(query_str)};
//
//    switch (result.size())
//    {
//        case 0:
//        {
//            throw Exception(
//                    ErrorCodes::BAD_ARGUMENTS,
//                    "Invalid configuration: replication_slot_name='{}', publication_name='{}'",
//                    replication_slot_name, publication_name);
//        }
//        case 1:
//        {
//            pqxx::row row{result[0]};
//            if (result[0][0].as<std::string>() == "slot_ok")
//                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid publication_name='{}'", publication_name);
//            else
//                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid replication_slot_name='{}'", replication_slot_name);
//        }
//    }
//
//    assert(result.size() == 2);
//    tx->commit();
//}
//

}
