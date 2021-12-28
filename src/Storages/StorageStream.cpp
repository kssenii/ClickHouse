#include <Storages/StorageStream.h>

#include <Interpreters/Context.h>
#include <Interpreters/InterpreterInsertQuery.h>
#include <Interpreters/InterpreterSelectQuery.h>

#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTInsertQuery.h>

#include <Processors/Sources/SourceFromSingleChunk.h>
#include <Processors/Transforms/ExpressionTransform.h>
#include <Processors/Executors/CompletedPipelineExecutor.h>

#include <Processors/Executors/PullingAsyncPipelineExecutor.h>
#include <Processors/Transforms/ExpressionTransform.h>
#include <Processors/Transforms/SquashingChunksTransform.h>
#include <Processors/Transforms/MaterializingTransform.h>
#include <Processors/Executors/PipelineExecutor.h>
#include <Processors/Sinks/EmptySink.h>

#include <Storages/LiveView/StorageBlocks.h>
#include <Storages/StorageFactory.h>

#include <base/logger_useful.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_QUERY;
    extern const int QUERY_NOT_ALLOWED;
}

StorageStream::StorageStream(
    const StorageID & table_id_,
    ContextPtr local_context,
    const ASTCreateQuery & query,
    const ColumnsDescription & columns_,
    bool attach_)
    : IStorage(table_id_), WithMutableContext(local_context->getGlobalContext())
    , log(&Poco::Logger::get(fmt::format("StorageStream({}.{})", table_id_.database_name, table_id_.table_name)))
{
    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(columns_);

    if (!query.select)
        throw Exception(ErrorCodes::INCORRECT_QUERY, "SELECT query is not specified for {}", getName());

    if (query.select->list_of_selects->children.size() != 1)
        throw Exception(ErrorCodes::QUERY_NOT_ALLOWED, "UNION is not supported for {}", getName());

    inner_query = query.select->list_of_selects->children.at(0);
    auto select = SelectQueryDescription::getSelectQueryFromASTForMatView(query.select->clone(), local_context);
    storage_metadata.setSelectQuery(select);
    setInMemoryMetadata(storage_metadata);

    if (!select.select_table_id.empty())
    {
        select_table_id = select.select_table_id;
        DatabaseCatalog::instance().addDependency(select_table_id, table_id_);
    }

    if (attach_) {}
}

void StorageStream::startup()
{
}

void StorageStream::shutdown()
{
    auto table_id = getStorageID();
    DatabaseCatalog::instance().removeDependency(select_table_id, table_id);
}

StorageStream::Subscriptions StorageStream::getSubscriptions(const StorageStream & stream)
{
    std::lock_guard lock(stream.subscriptions_mutex);
    return stream.subscriptions;
}

void StorageStream::subscribe(const StorageID & subscriber, bool if_not_subscribed)
{
    std::lock_guard lock(subscriptions_mutex);
    auto subscribed = subscriptions.contains(subscriber);
    if (subscribed)
    {
        if (if_not_subscribed)
            return;
        throw Exception(ErrorCodes::INCORRECT_QUERY, "Subscription already created");
    }
    subscriptions.insert(subscriber);
}

void StorageStream::unsubscribe(const StorageID & subscriber, bool if_subscribed)
{
    std::lock_guard lock(subscriptions_mutex);
    auto subscribed = subscriptions.contains(subscriber);
    if (!subscribed)
    {
        if (if_subscribed)
            return;
        throw Exception(ErrorCodes::INCORRECT_QUERY, "Cannot ubsubscribe because there is no subscription");
    }
    subscriptions.erase(subscriber);
}

void registerStorageStream(StorageFactory & factory)
{
    factory.registerStorage("Stream", [](const StorageFactory::Arguments & args)
    {
        return StorageStream::create(args.table_id, args.getLocalContext(), args.query, args.columns, args.attach);
    });
}

void StorageStream::writeIntoStream(const Block & block, ContextPtr local_context)
{
    for (const auto & subscription : subscriptions)
    {
        switch (strategy)
        {
            case FlushStrategy::DEFAULT:
            {
                Pipe pipe = Pipe(std::make_shared<SourceFromSingleChunk>(block));
                writeIntoStorage(subscription, std::move(pipe), local_context);
                break;
            }
            case FlushStrategy::INNER_QUERY_RESULT_UPDATE:
            {
                break;
            }
            case FlushStrategy::TIME_WINDOW:
            {
                break;
            }
        }
    }
}

void StorageStream::writeIntoStorage(
    const StorageID & target_storage_id, Pipe pipe, ContextPtr local_context)
{
    auto query = inner_query->clone();
    InterpreterSelectQuery select(query, local_context, std::move(pipe), QueryProcessingStage::Complete);
    auto builder = select.buildQueryPipeline();
    auto target_table = DatabaseCatalog::instance().getTable(target_storage_id, local_context);
    auto result_header = target_table->getInMemoryMetadataPtr()->getSampleBlock();

    auto convert_actions_dag = ActionsDAG::makeConvertingActions(
        builder.getHeader().getColumnsWithTypeAndName(),
        result_header.getColumnsWithTypeAndName(),
        ActionsDAG::MatchColumnsMode::Position);
    auto actions = std::make_shared<ExpressionActions>(
        convert_actions_dag,
        ExpressionActionsSettings::fromContext(local_context, CompileExpressions::yes));
    builder.addSimpleTransform([&](const Block & stream_header)
    {
        return std::make_shared<ExpressionTransform>(stream_header, actions);
    });

    auto insert = std::make_shared<ASTInsertQuery>();
    insert->table_id = target_storage_id;
    InterpreterInsertQuery interpreter(insert, local_context);
    auto block_io = interpreter.execute();

    auto result = QueryPipelineBuilder::getPipe(std::move(builder));
    block_io.pipeline.complete(std::move(result));
    CompletedPipelineExecutor executor(block_io.pipeline);
    executor.execute();
}

}
