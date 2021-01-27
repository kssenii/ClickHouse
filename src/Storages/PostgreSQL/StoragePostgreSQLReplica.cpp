#include "StoragePostgreSQLReplica.h"

#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeArray.h>

#include <Databases/DatabaseOnDisk.h>
#include <Formats/FormatFactory.h>
#include <Formats/FormatSettings.h>

#include <Common/Macros.h>
#include <Core/Settings.h>

#include <Common/parseAddress.h>
#include <Common/assert_cast.h>

#include <Processors/Sources/SourceFromInputStream.h>
#include <Processors/Pipe.h>
#include <DataStreams/ConvertingBlockInputStream.h>
#include <Interpreters/executeQuery.h>

#include <Storages/StorageFactory.h>

#include "PostgreSQLReplicationSettings.h"
#include "PostgreSQLReplicaBlockInputStream.h"


namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

StoragePostgreSQLReplica::StoragePostgreSQLReplica(
    const StorageID & table_id_,
    const String & remote_table_name_,
    const ColumnsDescription & columns_,
    const ConstraintsDescription & constraints_,
    const String & relative_data_path_,
    const Context & context_,
    const PostgreSQLReplicationHandler & replication_handler_,
    std::unique_ptr<PostgreSQLReplicationSettings> replication_settings_)
    : IStorage(table_id_)
    , remote_table_name(remote_table_name_)
    , global_context(context_)
    , replication_settings(std::move(replication_settings_))
    , replication_handler(std::make_unique<PostgreSQLReplicationHandler>(replication_handler_))
{
    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(columns_);
    storage_metadata.setConstraints(constraints_);
    setInMemoryMetadata(storage_metadata);

    /// Helper table (ReplacingMergeTree).
    const auto ast_create = getCreateHelperTableQuery()->as<const ASTCreateQuery &>();
    Context context_copy(global_context);
    StoragePtr table = createTableFromAST(ast_create, table_id_.database_name, relative_data_path_, context_copy, false).second;
}


//Context StoragePostgreSQLReplica::createQueryContext()
//{
//    //Settings new_query_settings = context.getSettings();
//    //new_query_settings.insert_allow_materialized_columns = true;
//    //CurrentThread::QueryScope query_scope(query_context);
//
//    //Context query_context(global_context);
//    //query_context.setSettings(new_query_settings);
//
//    //query_context.getClientInfo().query_kind = ClientInfo::QueryKind::SECONDARY_QUERY;
//    //query_context.setCurrentQueryId(""); // generate random query_id
//    //return query_context;
//    re
//}


std::shared_ptr<ASTColumnDeclaration> StoragePostgreSQLReplica::getMaterializedColumnsDeclaration(
        const String name, const String type, UInt64 default_value)
{
    auto column_declaration = std::make_shared<ASTColumnDeclaration>();

    column_declaration->name = name;
    column_declaration->type = makeASTFunction(type);

    column_declaration->default_specifier = "MATERIALIZED";
    column_declaration->default_expression = std::make_shared<ASTLiteral>(default_value);

    column_declaration->children.emplace_back(column_declaration->type);
    column_declaration->children.emplace_back(column_declaration->default_expression);

    return column_declaration;
}


ASTPtr StoragePostgreSQLReplica::getColumnDeclaration(const DataTypePtr & data_type)
{
    WhichDataType which(data_type);

    if (which.isNullable())
        return makeASTFunction("Nullable", getColumnDeclaration(typeid_cast<const DataTypeNullable *>(data_type.get())->getNestedType()));

    if (which.isArray())
        return makeASTFunction("Array", getColumnDeclaration(typeid_cast<const DataTypeArray *>(data_type.get())->getNestedType()));

    return std::make_shared<ASTIdentifier>(data_type->getName());
}


std::shared_ptr<ASTColumns> StoragePostgreSQLReplica::getColumnsListFromStorage()
{
    auto columns_declare_list = std::make_shared<ASTColumns>();

    auto columns_expression_list = std::make_shared<ASTExpressionList>();
    auto metadata_snapshot = getInMemoryMetadataPtr();
    for (const auto & column_type_and_name : metadata_snapshot->getColumns().getOrdinary())
    {
        const auto & column_declaration = std::make_shared<ASTColumnDeclaration>();
        column_declaration->name = column_type_and_name.name;
        column_declaration->type = getColumnDeclaration(column_type_and_name.type);
        columns_expression_list->children.emplace_back(column_declaration);
    }
    columns_declare_list->set(columns_declare_list->columns, columns_expression_list);

    columns_declare_list->columns->children.emplace_back(getMaterializedColumnsDeclaration("_sign", "Int8", UInt64(1)));
    columns_declare_list->columns->children.emplace_back(getMaterializedColumnsDeclaration("_version", "UInt64", UInt64(1)));

    return columns_declare_list;
}


ASTPtr StoragePostgreSQLReplica::getCreateHelperTableQuery()
{
    auto create_table_query = std::make_shared<ASTCreateQuery>();

    auto table_id = getStorageID();
    create_table_query->table = table_id.table_name + "_ReplacingMergeTree";
    create_table_query->database = table_id.database_name;

    create_table_query->set(create_table_query->columns_list, getColumnsListFromStorage());

    auto storage = std::make_shared<ASTStorage>();
    storage->set(storage->engine, makeASTFunction("ReplacingMergeTree", std::make_shared<ASTIdentifier>("_version")));

    auto metadata_snapshot = getInMemoryMetadataPtr();
    if (metadata_snapshot->hasSortingKey())
        storage->set(storage->order_by, metadata_snapshot->getSortingKey().expression_list_ast);
    else
        throw Exception("Storage PostgreSQLReplica requires order by key", ErrorCodes::BAD_ARGUMENTS);

    //const auto & create_defines = create_query.columns_list->as<MySQLParser::ASTCreateDefines>();

    //NamesAndTypesList columns_name_and_type = getColumnsList(create_defines->columns);
    //const auto & [primary_keys, unique_keys, keys, increment_columns] = getKeys(create_defines->columns, create_defines->indices, context, columns_name_and_type);

    ///// The `partition by` expression must use primary keys, otherwise the primary keys will not be merge.
    //ASTPtr partition_expression = getPartitionPolicy(primary_keys);
    ///// The `order by` expression must use primary keys, otherwise the primary keys will not be merge.
    //ASTPtr order_by_expression = getOrderByPolicy(primary_keys, unique_keys, keys, increment_columns);

    //if (partition_expression)
    //    storage->set(storage->partition_by, partition_expression);
    //if (order_by_expression)

    create_table_query->set(create_table_query->storage, storage);

    return create_table_query;
}


void StoragePostgreSQLReplica::startup()
{
    replication_handler->startup();
}


void StoragePostgreSQLReplica::shutdown()
{
}


Pipe StoragePostgreSQLReplica::read(
        const Names & column_names,
        const StorageMetadataPtr & metadata_snapshot,
        SelectQueryInfo & /* query_info */,
        const Context & /* context */,
        QueryProcessingStage::Enum /* processed_stage */,
        size_t /* max_block_size */,
        unsigned /* num_streams */)
{
    auto sample_block = metadata_snapshot->getSampleBlockForColumns(column_names, getVirtuals(), getStorageID());
    return Pipe();
}


NamesAndTypesList StoragePostgreSQLReplica::getVirtuals() const
{
    return NamesAndTypesList{
    };
}


void registerStoragePostgreSQLReplica(StorageFactory & factory)
{
    auto creator_fn = [](const StorageFactory::Arguments & args)
    {
        ASTs & engine_args = args.engine_args;
        bool has_settings = args.storage_def->settings;
        auto postgresql_replication_settings = std::make_unique<PostgreSQLReplicationSettings>();

        if (has_settings)
            postgresql_replication_settings->loadFromQuery(*args.storage_def);

        if (engine_args.size() != 5)
            throw Exception("Storage PostgreSQLReplica requires 5 parameters: "
                            "PostgreSQL('host:port', 'database', 'table', 'username', 'password'",
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        for (auto & engine_arg : engine_args)
            engine_arg = evaluateConstantExpressionOrIdentifierAsLiteral(engine_arg, args.local_context);

        auto parsed_host_port = parseAddress(engine_args[0]->as<ASTLiteral &>().value.safeGet<String>(), 5432);
        const String & remote_table = engine_args[2]->as<ASTLiteral &>().value.safeGet<String>();
        const String & remote_database = engine_args[1]->as<ASTLiteral &>().value.safeGet<String>();

        String connection_str;
        connection_str = fmt::format("dbname={} host={} port={} user={} password={}",
                remote_database,
                parsed_host_port.first, std::to_string(parsed_host_port.second),
                engine_args[3]->as<ASTLiteral &>().value.safeGet<String>(),
                engine_args[4]->as<ASTLiteral &>().value.safeGet<String>());

        auto global_context(args.context.getGlobalContext());
        auto replication_slot_name = global_context.getMacros()->expand(postgresql_replication_settings->postgresql_replication_slot_name.value);
        auto publication_name = global_context.getMacros()->expand(postgresql_replication_settings->postgresql_publication_name.value);

        PostgreSQLReplicationHandler replication_handler(global_context, remote_database, remote_table, connection_str, replication_slot_name, publication_name);

        return StoragePostgreSQLReplica::create(
                args.table_id, remote_table, args.columns, args.constraints, args.relative_data_path, global_context,
                replication_handler, std::move(postgresql_replication_settings));
    };

    factory.registerStorage(
            "PostgreSQLReplica",
            creator_fn,
            StorageFactory::StorageFeatures{ .supports_settings = true, .supports_sort_order = true, .source_access_type = AccessType::POSTGRES,
    });
}

}

