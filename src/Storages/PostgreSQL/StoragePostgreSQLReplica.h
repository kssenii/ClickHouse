#pragma once

#include "config_core.h"

#include "PostgreSQLReplicationHandler.h"
#include "PostgreSQLReplicaSettings.h"

#include <Parsers/IAST.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTColumnDeclaration.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterCreateQuery.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <ext/shared_ptr_helper.h>


namespace DB
{

class StoragePostgreSQLReplica final : public ext::shared_ptr_helper<StoragePostgreSQLReplica>, public IStorage
{
    friend struct ext::shared_ptr_helper<StoragePostgreSQLReplica>;

public:
    String getName() const override { return "PostgreSQLReplica"; }

    void startup() override;
    void shutdown() override;

    NamesAndTypesList getVirtuals() const override;

    Pipe read(
        const Names & column_names,
        const StorageMetadataPtr & metadata_snapshot,
        SelectQueryInfo & query_info,
        const Context & context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        unsigned num_streams) override;

    /// Called right after shutdown() in case of drop query
    void shutdownFinal();

protected:
    StoragePostgreSQLReplica(
        const StorageID & table_id_,
        const String & remote_database_name,
        const String & remote_table_name,
        const String & connection_str,
        const String & relative_data_path_,
        const StorageInMemoryMetadata & storage_metadata,
        const Context & context_,
        std::unique_ptr<PostgreSQLReplicaSettings> replication_settings_);

private:
    std::shared_ptr<ASTColumnDeclaration> getMaterializedColumnsDeclaration(
            const String name, const String type, UInt64 default_value);
    std::shared_ptr<ASTColumns> getColumnsListFromStorage();
    ASTPtr getColumnDeclaration(const DataTypePtr & data_type);
    ASTPtr getCreateHelperTableQuery();
    void dropNested();

    std::string remote_table_name, relative_data_path;
    std::shared_ptr<Context> global_context;

    std::unique_ptr<PostgreSQLReplicaSettings> replication_settings;
    std::unique_ptr<PostgreSQLReplicationHandler> replication_handler;

    /// ReplacingMergeTree table
    StoragePtr nested_storage;
};

}

