#include "PostgreSQLReplicaConsumer.h"

#include <Formats/FormatFactory.h>
#include <Formats/FormatSettings.h>

#include <ext/range.h>

#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

#include <Common/FieldVisitors.h>
#include <Common/hex.h>

#include <Interpreters/Context.h>
#include <Interpreters/InterpreterInsertQuery.h>

#include <DataStreams/copyData.h>
#include <DataStreams/OneBlockInputStream.h>

#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnDecimal.h>

#include <DataTypes/IDataType.h>
#include <DataTypes/DataTypeNullable.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

static const auto RESCHEDULE_MS = 500;
static const auto MAX_EMPTY_READS = 20;
static const auto MAX_EMPTY_READS_BEFORE_FLUSH = 2;
static const auto MAX_THREAD_WORK_DURATION_MS = 60000;


PostgreSQLReplicaConsumer::PostgreSQLReplicaConsumer(
    std::shared_ptr<Context> context_,
    const std::string & table_name_,
    PostgreSQLConnectionPtr connection_,
    const std::string & replication_slot_name_,
    const std::string & publication_name_,
    const std::string & metadata_path,
    const LSNPosition & start_lsn,
    const size_t max_block_size_,
    StoragePtr nested_storage_)
    : log(&Poco::Logger::get("PostgreSQLReaplicaConsumer"))
    , context(context_)
    , replication_slot_name(replication_slot_name_)
    , publication_name(publication_name_)
    , metadata(metadata_path)
    , table_name(table_name_)
    , connection(std::move(connection_))
    , current_lsn(start_lsn)
    , max_block_size(max_block_size_)
    , nested_storage(nested_storage_)
    , sample_block(nested_storage->getInMemoryMetadata().getSampleBlock())
{
    description.init(sample_block);
    for (const auto idx : ext::range(0, description.sample_block.columns()))
        if (description.types[idx].first == ExternalResultDescription::ValueType::vtArray)
            preparePostgreSQLArrayInfo(array_info, idx, description.sample_block.getByPosition(idx).type);

    wal_reader_task = context->getSchedulePool().createTask("PostgreSQLReplicaWALReader", [this]{ replicationStream(); });
    wal_reader_task->deactivate();

}


void PostgreSQLReplicaConsumer::startSynchronization()
{
    wal_reader_task->activateAndSchedule();
}


void PostgreSQLReplicaConsumer::stopSynchronization()
{
    stop_synchronization.store(true);
    wal_reader_task->deactivate();
}


void PostgreSQLReplicaConsumer::replicationStream()
{
    auto start_time = std::chrono::steady_clock::now();
    metadata.readMetadata();

    LOG_TRACE(log, "Starting replication stream");

    while (!stop_synchronization)
    {
        if (!streamChanges())
        {
            LOG_TRACE(log, "Reschedule replication stream. Replication slot is empty.");
            break;
        }

        auto end_time = std::chrono::steady_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
        if (duration.count() > MAX_THREAD_WORK_DURATION_MS)
        {
            LOG_TRACE(log, "Reschedule replication_stream. Thread work duration limit exceeded.");
            break;
        }
    }

    if (!stop_synchronization)
        wal_reader_task->scheduleAfter(RESCHEDULE_MS);
}


void PostgreSQLReplicaConsumer::insertValue(std::string & value, size_t column_idx)
{
    LOG_TRACE(log, "INSERTING VALUE {}", value);
    const auto & sample = description.sample_block.getByPosition(column_idx);
    bool is_nullable = description.types[column_idx].second;

    if (is_nullable)
    {
        ColumnNullable & column_nullable = assert_cast<ColumnNullable &>(*columns[column_idx]);
        const auto & data_type = assert_cast<const DataTypeNullable &>(*sample.type);

        insertPostgreSQLValue(
                column_nullable.getNestedColumn(), value,
                description.types[column_idx].first, data_type.getNestedType(), array_info, column_idx);

        column_nullable.getNullMapData().emplace_back(0);
    }
    else
    {
        insertPostgreSQLValue(
                *columns[column_idx], value, description.types[column_idx].first, sample.type, array_info, column_idx);
    }
}


void PostgreSQLReplicaConsumer::insertDefaultValue(size_t column_idx)
{
    const auto & sample = description.sample_block.getByPosition(column_idx);
    insertDefaultPostgreSQLValue(*columns[column_idx], *sample.column);
}


void PostgreSQLReplicaConsumer::readString(const char * message, size_t & pos, size_t size, String & result)
{
    assert(size > pos + 2);
    char current = unhex2(message + pos);
    pos += 2;
    while (pos < size && current != '\0')
    {
        result += current;
        current = unhex2(message + pos);
        pos += 2;
    }
}


Int32 PostgreSQLReplicaConsumer::readInt32(const char * message, size_t & pos)
{
    assert(size > pos + 8);
    Int32 result = (UInt32(unhex2(message + pos)) << 24)
                | (UInt32(unhex2(message + pos + 2)) << 16)
                | (UInt32(unhex2(message + pos + 4)) << 8)
                | (UInt32(unhex2(message + pos + 6)));
    pos += 8;
    return result;
}


Int16 PostgreSQLReplicaConsumer::readInt16(const char * message, size_t & pos)
{
    assert(size > pos + 4);
    Int16 result = (UInt32(unhex2(message + pos)) << 8)
                | (UInt32(unhex2(message + pos + 2)));
    pos += 4;
    return result;
}


Int8 PostgreSQLReplicaConsumer::readInt8(const char * message, size_t & pos)
{
    assert(size > pos + 2);
    Int8 result = unhex2(message + pos);
    pos += 2;
    return result;
}


Int64 PostgreSQLReplicaConsumer::readInt64(const char * message, size_t & pos)
{
    assert(size > pos + 16);
    Int64 result = (UInt64(unhex4(message + pos)) << 48)
                | (UInt64(unhex4(message + pos + 4)) << 32)
                | (UInt64(unhex4(message + pos + 8)) << 16)
                | (UInt64(unhex4(message + pos + 12)));
    pos += 16;
    return result;
}


void PostgreSQLReplicaConsumer::readTupleData(const char * message, size_t & pos, PostgreSQLQuery type, bool old_value)
{
    Int16 num_columns = readInt16(message, pos);
    /// 'n' means nullable, 'u' means TOASTed value, 't' means text formatted data
    LOG_DEBUG(log, "num_columns {}", num_columns);
    for (int column_idx = 0; column_idx < num_columns; ++column_idx)
    {
        char identifier = readInt8(message, pos);
        Int32 col_len = readInt32(message, pos);
        String value;
        for (int i = 0; i < col_len; ++i)
        {
            value += readInt8(message, pos);
        }

        insertValue(value, column_idx);

        LOG_DEBUG(log, "identifier {}, col_len {}, value {}", identifier, col_len, value);
    }

    switch (type)
    {
        case PostgreSQLQuery::INSERT:
        {
            columns[num_columns]->insert(Int8(1));
            columns[num_columns + 1]->insert(UInt64(metadata.version()));
            break;
        }
        case PostgreSQLQuery::DELETE:
        {
            columns[num_columns]->insert(Int8(-1));
            columns[num_columns + 1]->insert(UInt64(metadata.version()));
            break;
        }
        case PostgreSQLQuery::UPDATE:
        {
            if (old_value)
                columns[num_columns]->insert(Int8(-1));
            else
                columns[num_columns]->insert(Int8(1));

            columns[num_columns + 1]->insert(UInt64(metadata.version()));
            break;
        }
    }
}


void PostgreSQLReplicaConsumer::processReplicationMessage(const char * replication_message, size_t size, size_t & num_rows)
{
    /// Skip '\x'
    size_t pos = 2;
    char type = readInt8(replication_message, pos);

    LOG_TRACE(log, "TYPE: {}", type);
    switch (type)
    {
        case 'B': // Begin
        {
            Int64 transaction_end_lsn = readInt64(replication_message, pos);
            Int64 transaction_commit_timestamp = readInt64(replication_message, pos);
            LOG_DEBUG(log, "transaction lsn {}, transaction commit timespamp {}",
                    transaction_end_lsn, transaction_commit_timestamp);
            //current_lsn.lsn_value = transaction_end_lsn;
            break;
        }
        case 'C': // Commit
        {
            readInt8(replication_message, pos);
            Int64 commit_lsn = readInt64(replication_message, pos);
            Int64 transaction_end_lsn = readInt64(replication_message, pos);
            /// Since postgres epoch
            Int64 transaction_commit_timestamp = readInt64(replication_message, pos);
            LOG_DEBUG(log, "commit lsn {}, transaction lsn {}, transaction commit timestamp {}",
                    commit_lsn, transaction_end_lsn, transaction_commit_timestamp);
            final_lsn.lsn = current_lsn.lsn;
            break;
        }
        case 'O': // Origin
            break;
        case 'R': // Relation
        {
            Int32 relation_id = readInt32(replication_message, pos);
            String relation_namespace, relation_name;
            readString(replication_message, pos, size, relation_namespace);
            readString(replication_message, pos, size, relation_name);
            Int8 replica_identity = readInt8(replication_message, pos);
            Int16 num_columns = readInt16(replication_message, pos);

            LOG_DEBUG(log,
                    "Replication message type 'R', relation_id: {}, namespace: {}, relation name {}, replica identity {}, columns number {}",
                    relation_id, relation_namespace, relation_name, replica_identity, num_columns);

            Int8 key;
            Int32 data_type_id, type_modifier;
            for (uint16_t i = 0; i < num_columns; ++i)
            {
                String column_name;
                key = readInt8(replication_message, pos);
                readString(replication_message, pos, size, column_name);
                data_type_id = readInt32(replication_message, pos);
                type_modifier = readInt32(replication_message, pos);
                LOG_DEBUG(log, "Key {}, column name {}, data type id {}, type modifier {}", key, column_name, data_type_id, type_modifier);
            }

            break;
        }
        case 'Y': // Type
            break;
        case 'I': // Insert
        {
            Int32 relation_id = readInt32(replication_message, pos);
            Int8 new_tuple = readInt8(replication_message, pos);

            LOG_DEBUG(log, "relationID {}, newTuple {}", relation_id, new_tuple);
            readTupleData(replication_message, pos, PostgreSQLQuery::INSERT);
            ++num_rows;
            break;
        }
        case 'U': // Update
        {
            Int32 relation_id = readInt32(replication_message, pos);
            Int8 primary_key_or_old_tuple_data = readInt8(replication_message, pos);

            LOG_DEBUG(log, "relationID {}, key {}", relation_id, primary_key_or_old_tuple_data);

            readTupleData(replication_message, pos, PostgreSQLQuery::UPDATE, true);
            ++num_rows;

            if (pos + 1 < size)
            {
                Int8 new_tuple_data = readInt8(replication_message, pos);
                LOG_DEBUG(log, "new tuple data {}", new_tuple_data);
                readTupleData(replication_message, pos, PostgreSQLQuery::UPDATE);
                ++num_rows;
            }

            break;
        }
        case 'D': // Delete
        {
            Int32 relation_id = readInt32(replication_message, pos);
            //Int8 index_replica_identity = readInt8(replication_message, pos);
            Int8 full_replica_identity = readInt8(replication_message, pos);

            LOG_DEBUG(log, "relationID {}, full replica identity {}",
                    relation_id, full_replica_identity);
            //LOG_DEBUG(log, "relationID {}, index replica identity {} full replica identity {}",
            //        relation_id, index_replica_identity, full_replica_identity);
            readTupleData(replication_message, pos, PostgreSQLQuery::DELETE);
            ++num_rows;
            break;
        }
        case 'T': // Truncate
            break;
        default:
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                    "Unexpected byte1 value {} while parsing replication message", type);
    }
}


void PostgreSQLReplicaConsumer::syncIntoTable(Block & block)
{
    Context insert_context(*context);
    insert_context.makeQueryContext();

    auto insert = std::make_shared<ASTInsertQuery>();
    insert->table_id = nested_storage->getStorageID();

    InterpreterInsertQuery interpreter(insert, insert_context);
    auto block_io = interpreter.execute();
    OneBlockInputStream input(block);

    copyData(input, *block_io.out);
    LOG_TRACE(log, "TABLE SYNC END");
}


String PostgreSQLReplicaConsumer::advanceLSN(std::shared_ptr<pqxx::nontransaction> ntx)
{
    LOG_TRACE(log, "CURRENT LSN FROM TO {}", final_lsn.lsn);

    std::string query_str = fmt::format("SELECT end_lsn FROM pg_replication_slot_advance('{}', '{}')", replication_slot_name, final_lsn.lsn);
    pqxx::result result{ntx->exec(query_str)};

    ntx->commit();

    if (!result.empty())
        return result[0][0].as<std::string>();

    return final_lsn.lsn;
}


bool PostgreSQLReplicaConsumer::streamChanges()
{
    columns = description.sample_block.cloneEmptyColumns();
    std::shared_ptr<pqxx::nontransaction> tx;
    size_t num_rows = 0, count_empty_reads = 0;

    while (!stop_synchronization
            && num_rows < max_block_size
            && count_empty_reads < (num_rows ? MAX_EMPTY_READS_BEFORE_FLUSH : MAX_EMPTY_READS))
    {
        if (!readFromReplicationSlot(num_rows))
            ++count_empty_reads;
        else
            count_empty_reads = 0;

        LOG_TRACE(log, "kssenii num_rows {} max_block_size {} count_empty_reads {}",
                num_rows, max_block_size, count_empty_reads);
    }

    Block result_rows = description.sample_block.cloneWithColumns(std::move(columns));
    if (result_rows.rows())
    {
        tx = std::make_shared<pqxx::nontransaction>(*connection->conn());
        metadata.commitMetadata(final_lsn.lsn, [&]()
        {
            syncIntoTable(result_rows);
            advanceLSN(tx);
        });

        return true;
    }

    return false;
}


/// Read binary changes from replication slot via COPY command.
bool PostgreSQLReplicaConsumer::readFromReplicationSlot(size_t & num_rows)
{
    try
    {
        auto tx = std::make_shared<pqxx::nontransaction>(*connection->conn());

        std::string query_str = fmt::format(
                "select lsn, data FROM pg_logical_slot_get_binary_changes("
                "'{}', NULL, NULL, 'publication_names', '{}', 'proto_version', '1')",
                replication_slot_name, publication_name);

        pqxx::stream_from stream(*tx, pqxx::from_query, std::string_view(query_str));
        bool slot_empty = true;

        while (true)
        {
            const std::vector<pqxx::zview> * row{stream.read_row()};

            if (!row)
            {
                LOG_TRACE(log, "STREAM REPLICATION END");
                stream.complete();

                if (slot_empty)
                    return false;

                tx->commit();
                break;
            }

            slot_empty = false;

            current_lsn.lsn = (*row)[0];
            LOG_TRACE(log, "Replication message: {}", (*row)[1]);
            processReplicationMessage((*row)[1].c_str(), (*row)[1].size(), num_rows);
        }
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
        return false;
    }

    return true;
}

}


