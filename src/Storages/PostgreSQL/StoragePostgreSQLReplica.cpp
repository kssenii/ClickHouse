#include "StoragePostgreSQLReplica.h"

#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeArray.h>

#include <Databases/DatabaseOnDisk.h>
#include <Formats/FormatFactory.h>
#include <Formats/FormatSettings.h>

#include <Processors/Transforms/FilterTransform.h>
#include <Parsers/ASTTablesInSelectQuery.h>

#include <Common/Macros.h>
#include <Core/Settings.h>

#include <Common/parseAddress.h>
#include <Common/assert_cast.h>

#include <Processors/Sources/SourceFromInputStream.h>
#include <Processors/Pipe.h>
#include <DataStreams/ConvertingBlockInputStream.h>
#include <Interpreters/executeQuery.h>
#include <Interpreters/InterpreterSelectQuery.h>
#include <Interpreters/InterpreterDropQuery.h>

#include <Storages/StorageFactory.h>

#include "PostgreSQLReplicationSettings.h"
#include "PostgreSQLReplicaBlockInputStream.h"
#include <Databases/DatabaseOnDisk.h>

#include <common/logger_useful.h>
#include <Poco/File.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

static auto nested_storage_suffix = "_ReplacingMergeTree";

StoragePostgreSQLReplica::StoragePostgreSQLReplica(
    const StorageID & table_id_,
    const String & remote_database_name,
    const String & remote_table_name,
    const String & connection_str,
    const String & relative_data_path_,
    const StorageInMemoryMetadata & storage_metadata,
    const Context & context_,
    std::unique_ptr<PostgreSQLReplicationSettings> replication_settings_)
    : IStorage(table_id_)
    , relative_data_path(relative_data_path_)
    , global_context(std::make_shared<Context>(context_.getGlobalContext()))
    , replication_settings(std::move(replication_settings_))
{
    setInMemoryMetadata(storage_metadata);
    relative_data_path.resize(relative_data_path.size() - 1);
    relative_data_path += nested_storage_suffix;

    replication_handler = std::make_unique<PostgreSQLReplicationHandler>(
            remote_database_name,
            remote_table_name,
            connection_str,
            global_context,
            global_context->getMacros()->expand(replication_settings->postgresql_replication_slot_name.value),
            global_context->getMacros()->expand(replication_settings->postgresql_publication_name.value)
    );
}


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
    create_table_query->table = table_id.table_name + nested_storage_suffix;
    create_table_query->database = table_id.database_name;
    create_table_query->if_not_exists = true;

    create_table_query->set(create_table_query->columns_list, getColumnsListFromStorage());

    auto storage = std::make_shared<ASTStorage>();
    storage->set(storage->engine, makeASTFunction("ReplacingMergeTree", std::make_shared<ASTIdentifier>("_version")));

    auto primary_key_ast = getInMemoryMetadataPtr()->getPrimaryKeyAST();
    if (primary_key_ast)
        storage->set(storage->order_by, primary_key_ast);
    /// else

    //storage->set(storage->partition_by, ?);

    create_table_query->set(create_table_query->storage, storage);

    return create_table_query;
}


Pipe StoragePostgreSQLReplica::read(
        const Names & column_names,
        const StorageMetadataPtr & /* metadata_snapshot */,
        SelectQueryInfo & query_info,
        const Context & context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        unsigned num_streams)
{
    StoragePtr storage = DatabaseCatalog::instance().getTable(nested_storage->getStorageID(), *global_context);
    auto lock = nested_storage->lockForShare(context.getCurrentQueryId(), context.getSettingsRef().lock_acquire_timeout);

    const StorageMetadataPtr & nested_metadata = storage->getInMemoryMetadataPtr();
    Pipe pipe = storage->read(
            column_names,
            nested_metadata, query_info, context,
            processed_stage, max_block_size, num_streams);

    pipe.addTableLock(lock);
    return pipe;
}


void StoragePostgreSQLReplica::startup()
{
    Context context_copy(*global_context);
    const auto ast_create = getCreateHelperTableQuery();

    Poco::File path(relative_data_path);
    if (!path.exists())
    {
        LOG_TRACE(&Poco::Logger::get("StoragePostgreSQLReplica"),
                "Creating helper table {}", getStorageID().table_name + nested_storage_suffix);
        InterpreterCreateQuery interpreter(ast_create, context_copy);
        interpreter.execute();
    }
    else
        LOG_TRACE(&Poco::Logger::get("StoragePostgreSQLReplica"),
                "Directory already exists {}", relative_data_path);

    nested_storage = createTableFromAST(ast_create->as<const ASTCreateQuery &>(), getStorageID().database_name, relative_data_path, context_copy, false).second;
    nested_storage->startup();

    replication_handler->startup(nested_storage);
}


void StoragePostgreSQLReplica::shutdown()
{
    replication_handler->shutdown();
}


void StoragePostgreSQLReplica::shutdownFinal()
{
    /// TODO: Under lock? Make sure synchronization stopped.
    replication_handler->checkAndDropReplicationSlot();
    dropNested();
}


void StoragePostgreSQLReplica::dropNested()
{
    auto table_id = nested_storage->getStorageID();
    auto ast_drop = std::make_shared<ASTDropQuery>();

    ast_drop->kind = ASTDropQuery::Drop;
    ast_drop->table = table_id.table_name;
    ast_drop->database = table_id.database_name;
    ast_drop->if_exists = true;

    auto drop_context(*global_context);
    drop_context.makeQueryContext();

    auto interpreter = InterpreterDropQuery(ast_drop, drop_context);
    interpreter.execute();
}


NamesAndTypesList StoragePostgreSQLReplica::getVirtuals() const
{
    return NamesAndTypesList{};
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

        StorageInMemoryMetadata metadata;
        metadata.setColumns(args.columns);
        metadata.setConstraints(args.constraints);

        if (!args.storage_def->order_by && args.storage_def->primary_key)
            args.storage_def->set(args.storage_def->order_by, args.storage_def->primary_key->clone());

        if (!args.storage_def->order_by)
            throw Exception("Storage PostgreSQLReplica needs order by key or primary key", ErrorCodes::BAD_ARGUMENTS);

        if (args.storage_def->primary_key)
            metadata.primary_key = KeyDescription::getKeyFromAST(args.storage_def->primary_key->ptr(), metadata.columns, args.context);
        else
            metadata.primary_key = KeyDescription::getKeyFromAST(args.storage_def->order_by->ptr(), metadata.columns, args.context);

        auto parsed_host_port = parseAddress(engine_args[0]->as<ASTLiteral &>().value.safeGet<String>(), 5432);
        const String & remote_table = engine_args[2]->as<ASTLiteral &>().value.safeGet<String>();
        const String & remote_database = engine_args[1]->as<ASTLiteral &>().value.safeGet<String>();

        /// No connection is made here, see Storages/PostgreSQL/PostgreSQLConnection.cpp
        PostgreSQLConnection connection(
            remote_database,
            parsed_host_port.first,
            parsed_host_port.second,
            engine_args[3]->as<ASTLiteral &>().value.safeGet<String>(),
            engine_args[4]->as<ASTLiteral &>().value.safeGet<String>());

        return StoragePostgreSQLReplica::create(
                args.table_id, remote_database, remote_table, connection.conn_str(),
                args.relative_data_path, metadata, args.context,
                std::move(postgresql_replication_settings));
    };

    factory.registerStorage(
            "PostgreSQLReplica",
            creator_fn,
            StorageFactory::StorageFeatures{ .supports_settings = true, .supports_sort_order = true, .source_access_type = AccessType::POSTGRES,
    });
}

}

