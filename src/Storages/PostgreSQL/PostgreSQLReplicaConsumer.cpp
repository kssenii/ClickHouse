#include "PostgreSQLReplicaConsumer.h"

#include <Formats/FormatFactory.h>
#include <Formats/FormatSettings.h>
#include <ext/range.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Common/FieldVisitors.h>
#include <Common/hex.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int UNKNOWN_TABLE;
    extern const int LOGICAL_ERROR;
}

PostgreSQLReplicaConsumer::PostgreSQLReplicaConsumer(
    const std::string & table_name_,
    const std::string & conn_str,
    const std::string & replication_slot_name_,
    const std::string & publication_name_,
    const LSNPosition & start_lsn)
    : log(&Poco::Logger::get("PostgreSQLReaplicaConsumer"))
    , replication_slot_name(replication_slot_name_)
    , publication_name(publication_name_)
    , table_name(table_name_)
    , connection(std::make_shared<PostgreSQLConnection>(conn_str))
    , current_lsn(start_lsn)
{
    replication_connection = std::make_shared<PostgreSQLConnection>(fmt::format("{} replication=database", conn_str));
}


void PostgreSQLReplicaConsumer::createSubscription()
{
    //std::string query_str = fmt::format("CREATE SUBSCRIPTION {} ",
    //        table_name, connection->conn()->dbname(), publication_name);
    //auto tx = std::make_unique<pqxx::nontransaction>(*replication_connection->conn());
    //tx->exec(query_str);
    //tx->commit();
}


void PostgreSQLReplicaConsumer::run()
{
    auto tx = std::make_unique<pqxx::nontransaction>(*replication_connection->conn());
    /// up_to_lsn is set to NULL, up_to_n_changes is set to max_block_size.
    std::string query_str = fmt::format(
            "select data FROM pg_logical_slot_peek_binary_changes("
            "'{}', NULL, NULL, 'publication_names', '{}', 'proto_version', '1')",
            replication_slot_name, publication_name);
    pqxx::stream_from stream(*tx, pqxx::from_query, std::string_view(query_str));
    bool ok = false;

    while (true)
    {
        const std::vector<pqxx::zview> * row{stream.read_row()};

        if (!row)
        {
            LOG_TRACE(log, "STREAM REPLICATION END");
            stream.complete();
            tx->commit();
            break;
        }

        for (const auto idx : ext::range(0, row->size()))
        {
            LOG_TRACE(log, "Replication message: {}", (*row)[idx]);
            decodeReplicationMessage((*row)[idx].c_str(), (*row)[idx].size());
            ok = true;
        }
    }
    //auto options = fmt::format(" (\"proto_version\" '1', \"publication_names\" '{}')", publication_name);
    //startReplication(replication_slot_name, current_lsn.lsn, -1, options);
    if (ok)
        throw Exception("FINISH", ErrorCodes::UNKNOWN_TABLE);
}


void PostgreSQLReplicaConsumer::decodeReplicationMessage(const char * replication_message, size_t size)
{
    size_t pos = 2;
    assert(size > 2); /// '\x'
    char type = unhex2(replication_message + pos);
    pos += 2;

    LOG_TRACE(log, "TYPE: {}", type);
    switch (type)
    {
        case 'B': // Begin
            break;
        case 'C': // Commit
            break;
        case 'O': // Origin
            break;
        case 'R': // Relation
        {
            assert(size > pos + 8);
            int32_t relation_id = (UInt32(unhex2(replication_message + pos)) << 24)
                                | (UInt32(unhex2(replication_message + pos + 2)) << 16)
                                | (UInt32(unhex2(replication_message + pos + 4)) << 8)
                                | (UInt32(unhex2(replication_message + pos + 6)));
            pos += 8;
            String relation_namespace, relation_name;

            char current = unhex2(replication_message + pos);
            pos += 2;
            while (pos < size && current != '\0')
            {
                relation_namespace += current;
                current = unhex2(replication_message + pos);
                pos += 2;
            }

            current = unhex2(replication_message + pos);
            pos += 2;
            while (pos < size && current != '\0')
            {
                relation_name += current;
                current = unhex2(replication_message + pos);
                pos += 2;
            }

            LOG_DEBUG(log,
                    "Replication message type 'R', relation_id: {}, namespace: {}, relation name {}",
                    relation_id, relation_namespace, relation_name);
            break;
        }
        case 'Y': // Type
            break;
        case 'I': // Insert
            break;
        case 'U': // Update
            break;
        case 'D': // Delete
            break;
        case 'T': // Truncate
            break;
        default:
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                    "Unexpected byte1 value {} while parsing replication message", type);
    }
}

}


