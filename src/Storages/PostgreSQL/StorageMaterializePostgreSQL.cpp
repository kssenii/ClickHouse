#include "StorageMaterializePostgreSQL.h"

#include <Storages/StorageFactory.h>
#include <Storages/transformQueryForExternalDatabase.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Interpreters/Context.h>
#include <DataTypes/DataTypeString.h>
#include <DataStreams/PostgreSQLBlockInputStream.h>
#include <Core/Settings.h>
#include <Common/parseAddress.h>
#include <Common/assert_cast.h>
#include <Parsers/ASTLiteral.h>
#include <Columns/ColumnNullable.h>
#include <Formats/FormatFactory.h>
#include <Formats/FormatSettings.h>
#include <Processors/Sources/SourceFromInputStream.h>
#include <Processors/Pipe.h>
#include <IO/WriteHelpers.h>

#include "PostgreSQLReplicationHandler.h"

namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

StorageMaterializePostgreSQL::StorageMaterializePostgreSQL(
    const StorageID & table_id_,
    const String & remote_table_name_,
    const ColumnsDescription & columns_,
    const ConstraintsDescription & constraints_,
    const Context & context_)
    : IStorage(table_id_)
    , remote_table_name(remote_table_name_)
    , global_context(context_)
{
    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(columns_);
    storage_metadata.setConstraints(constraints_);
    setInMemoryMetadata(storage_metadata);

}


void registerStorageMaterializePostgreSQL(StorageFactory & factory)
{
    factory.registerStorage("MaterializePostgreSQL", [](const StorageFactory::Arguments & args)
    {
        ASTs & engine_args = args.engine_args;

        if (engine_args.size() != 7)
            throw Exception("Storage MaterializePostgreSQL requires 7 parameters: "
                            "PostgreSQL('host:port', 'database', 'table', 'username', 'password', "
                            "'replication_slot_name', 'publivation_name')",
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        for (auto & engine_arg : engine_args)
            engine_arg = evaluateConstantExpressionOrIdentifierAsLiteral(engine_arg, args.local_context);

        auto parsed_host_port = parseAddress(engine_args[0]->as<ASTLiteral &>().value.safeGet<String>(), 5432);
        const String & remote_table = engine_args[2]->as<ASTLiteral &>().value.safeGet<String>();

        String connection_str;
        connection_str = fmt::format("dbname={} host={} port={} user={} password={}",
                engine_args[1]->as<ASTLiteral &>().value.safeGet<String>(),
                parsed_host_port.first, std::to_string(parsed_host_port.second),
                engine_args[3]->as<ASTLiteral &>().value.safeGet<String>(),
                engine_args[4]->as<ASTLiteral &>().value.safeGet<String>());

        auto replication_slot_name = engine_args[5]->as<ASTLiteral &>().value.safeGet<String>();
        auto publication_name = engine_args[6]->as<ASTLiteral &>().value.safeGet<String>();
        PostgreSQLReplicationHandler handler(connection_str, replication_slot_name, publication_name);

        return StorageMaterializePostgreSQL::create(
            args.table_id, remote_table, args.columns, args.constraints, args.context);
    },
    {
        .source_access_type = AccessType::POSTGRES,
    });
}

}

