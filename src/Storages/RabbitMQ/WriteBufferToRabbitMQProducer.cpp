#include <Storages/RabbitMQ/WriteBufferToRabbitMQProducer.h>
#include "Core/Block.h"
#include "Columns/ColumnString.h"
#include "Columns/ColumnsNumber.h"
#include <common/logger_useful.h>
#include <amqpcpp.h>
#include <chrono>
#include <thread>

namespace DB
{

WriteBufferToRabbitMQProducer::WriteBufferToRabbitMQProducer(
        ChannelPtr producer_channel_,
        RabbitMQHandler & eventHandler_,
        const String & routing_key_,
        const String & exchange_,
        Poco::Logger * log_,
        const size_t num_queues_,
        const bool bind_by_id_,
        const bool hash_exchange_,
        std::optional<char> delimiter,
        size_t rows_per_message,
        size_t chunk_size_)
        : WriteBuffer(nullptr, 0)
        , producer_channel(std::move(producer_channel_))
        , eventHandler(eventHandler_)
        , routing_key(routing_key_)
        , exchange_name(exchange_)
        , log(log_)
        , num_queues(num_queues_)
        , bind_by_id(bind_by_id_)
        , hash_exchange(hash_exchange_)
        , delim(delimiter)
        , max_rows(rows_per_message)
        , chunk_size(chunk_size_)
{
    channel_id = std::to_string(producer_channel->id());
    checkExchange();
}


WriteBufferToRabbitMQProducer::~WriteBufferToRabbitMQProducer()
{
    LOG_TRACE(log, "Producer {} send {} messages", channel_id, cnt_sent);
    producer_channel->close();

    assert(rows == 0 && chunks.empty());
}


void WriteBufferToRabbitMQProducer::checkExchange()
{
    /* The AMQP::passive flag indicates that it should only be checked if such exchange already exists.
     * If it doesn't - then no queue bindings happened and publishing to an exchange, without any queue
     * bound to it, will lead to messages being routed nowhere. May be this check seems pointless, but
     * without it no publishing will happen.
     */
    local_exchange = exchange_name + channel_id;
    producer_channel->declareExchange(exchange_name, AMQP::fanout, AMQP::passive)
    .onSuccess([&]()
    {
        exchange_declared = true;
    })
    .onError([&](const char * message)
    {
        exchange_error = true;
        exchange_declared = false;
        LOG_ERROR(log, "Exchange was not declared: {}", message);
    });
}


void WriteBufferToRabbitMQProducer::count_row()
{
    if (++rows % max_rows == 0)
    {
        const std::string & last_chunk = chunks.back();
        size_t last_chunk_size = offset();

        if (delim && last_chunk[last_chunk_size - 1] == delim)
            --last_chunk_size;

        std::string payload;
        payload.reserve((chunks.size() - 1) * chunk_size + last_chunk_size);

        for (auto i = chunks.begin(), e = --chunks.end(); i != e; ++i)
            payload.append(*i);

        payload.append(last_chunk, 0, last_chunk_size);

        next_queue = next_queue % num_queues + 1;

        ++cnt_sent;

        /// it is important to make sure exchange is declared before we proceed
        while (!exchange_declared && !exchange_error)
        {
            startEventLoop(exchange_declared);
        }

        if (hash_exchange)
        {
            /* If hash exchange is used - it distributes messages among queues based on hash of a routing key.
             * To make it unique - use current channel id.
             */
            producer_channel->publish(exchange_name, channel_id, payload);
            LOG_TRACE(log, "Producer {} send to queue {} from exchange {}", channel_id, channel_id, local_exchange);
        }
        else if (bind_by_id)
        {
            producer_channel->publish(exchange_name, std::to_string(next_queue), payload);
            LOG_TRACE(log, "Producer {} send to queue {} from exchange {}", channel_id, next_queue, local_exchange);
        }
        else
        {
            producer_channel->publish(exchange_name, routing_key, payload);
        }

        rows = 0;
        chunks.clear();
        set(nullptr, 0);
    }
}


void WriteBufferToRabbitMQProducer::nextImpl()
{
    chunks.push_back(std::string());
    chunks.back().resize(chunk_size);
    set(chunks.back().data(), chunk_size);
}


void WriteBufferToRabbitMQProducer::startEventLoop(std::atomic<bool> & check_param)
{
    eventHandler.start(check_param);
}

}
