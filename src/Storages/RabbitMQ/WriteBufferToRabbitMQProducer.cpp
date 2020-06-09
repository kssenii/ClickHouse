#include <Storages/RabbitMQ/WriteBufferToRabbitMQProducer.h>
#include "Core/Block.h"
#include "Columns/ColumnString.h"
#include "Columns/ColumnsNumber.h"
#include <common/logger_useful.h>
#include <amqpcpp.h>
#include <chrono>
#include <thread>
#include <atomic>


namespace DB
{

enum
{
    Connection_setup_sleep = 200,
    Buffer_limit_to_flush = 10000,
    Loop_retries_limit = 1000,
    Loop_wait_sleep = 10,
    batch = 10000
};

WriteBufferToRabbitMQProducer::WriteBufferToRabbitMQProducer(
        std::pair<String, UInt16> & parsed_address,
        std::pair<String, String> & login_password_,
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
        , login_password(login_password_)
        , routing_key(routing_key_)
        , exchange_name(exchange_)
        , log(log_)
        , num_queues(num_queues_)
        , bind_by_id(bind_by_id_)
        , hash_exchange(hash_exchange_)
        , delim(delimiter)
        , max_rows(rows_per_message)
        , chunk_size(chunk_size_)
        , producerEvbase(event_base_new())
        , eventHandler(producerEvbase, log)
        , connection(&eventHandler, AMQP::Address(parsed_address.first, parsed_address.second,
                    AMQP::Login(login_password.first, login_password.second), "/"))
{
    /* The reason behind making a separate connection for each concurrent producer is explained here:
     * https://github.com/CopernicaMarketingSoftware/AMQP-CPP/issues/128#issuecomment-300780086 - publishing from
     * different threads (as outputStreams are asynchronous) with the same connection leads to internal library errors.
     */
    size_t cnt_retries = 0;
    while (!connection.ready() && ++cnt_retries != Loop_retries_limit)
    {
        event_base_loop(producerEvbase, EVLOOP_NONBLOCK | EVLOOP_ONCE);
        std::this_thread::sleep_for(std::chrono::milliseconds(Connection_setup_sleep));
    }

    if (!connection.ready())
    {
        LOG_ERROR(log, "Cannot set up connection for producer!");
    }

    producer_channel = std::make_shared<AMQP::TcpChannel>(&connection);
    checkExchange();

    producer_channel->confirmSelect().onSuccess([&]()
    {
        confirm_mode_set = true;
    })
    .onAck([&](int64_t /* deliverTag */, bool /* multiple */)
    {
        ++published;
    })
    .onNack([&](uint64_t /* deliveryTag */, bool /* multiple */, bool /* requeue */)
    {
        /// A case that should not normally happen at all. (It is not advised to republish in this case)
        LOG_ERROR(log, "Broker denied message publication.");
    });

    producerSetUp();
}


WriteBufferToRabbitMQProducer::~WriteBufferToRabbitMQProducer()
{
    proccessBatch();

    producer_channel->close();
    connection.close();

    assert(rows == 0 && chunks.empty());
}


void WriteBufferToRabbitMQProducer::countRow()
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

        rows = 0;
        chunks.clear();
        set(nullptr, 0);

        next_queue = next_queue % num_queues + 1;

        if (bind_by_id || hash_exchange)
        {
            producer_channel->publish(exchange_name, std::to_string(next_queue), payload);
        }
        else
        {
            producer_channel->publish(exchange_name, routing_key, payload);
        }

        ++message_counter;

        if (message_counter % batch == 0)
        {
            proccessBatch();
        }
    }
}

void WriteBufferToRabbitMQProducer::checkExchange()
{
    /* The AMQP::passive flag indicates that it should only be checked if there is a valid exchange with the given name
     * and makes it initialized on the current channel.
     */
    producer_channel->declareExchange(exchange_name, AMQP::fanout, AMQP::passive)
    .onSuccess([&]()
    {
        exchange_declared = true;
    })
    .onError([&](const char * message)
    {
        exchange_error = true;
        LOG_ERROR(log, "Exchange was not declared: {}", message);
    });
}


void WriteBufferToRabbitMQProducer::producerSetUp()
{
    size_t cnt_retries = 0;

    /// Cannot publish before producers confirm mode is not set up
    while (!confirm_mode_set && ++cnt_retries <= Loop_retries_limit)
    {
        startEventLoop();

        if (!confirm_mode_set)
        {
            std::this_thread::sleep_for(std::chrono::milliseconds(Loop_wait_sleep));
        }
    }

    /* Exchange also needs to be initialized before publishing (should already be initialized in a previous
     * loop, but check anyway.
     */
    while (!exchange_declared && !exchange_error)
    {
        startEventLoop();
    }
}


void WriteBufferToRabbitMQProducer::proccessBatch()
{
    while (published < message_counter)
    {
        startEventLoop();
    }
}


void WriteBufferToRabbitMQProducer::nextImpl()
{
    chunks.push_back(std::string());
    chunks.back().resize(chunk_size);
    set(chunks.back().data(), chunk_size);
}


void WriteBufferToRabbitMQProducer::startEventLoop()
{
    eventHandler.startProducerLoop();
}

}
