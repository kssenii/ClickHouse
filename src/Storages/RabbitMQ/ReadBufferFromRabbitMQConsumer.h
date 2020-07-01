#pragma once

#include <Core/Names.h>
#include <Core/Types.h>
#include <IO/ReadBuffer.h>
#include <amqpcpp.h>
#include <Storages/RabbitMQ/RabbitMQHandler.h>
#include <Core/BackgroundSchedulePool.h>
#include <Common/ConcurrentBoundedQueue.h>
#include <Interpreters/Context.h>

namespace Poco
{
    class Logger;
}

namespace DB
{

using ChannelPtr = std::shared_ptr<AMQP::TcpChannel>;
using HandlerPtr = std::shared_ptr<RabbitMQHandler>;

class ReadBufferFromRabbitMQConsumer : public ReadBuffer
{

public:
    ReadBufferFromRabbitMQConsumer(
            std::pair<String, UInt16> & parsed_address,
            Context & global_context,
            std::pair<String, String> & login_password,
            const String & exchange_name_,
            const Names & routing_keys_,
            const size_t channel_id_,
            Poco::Logger * log_,
            char row_delimiter_,
            const bool bind_by_id_,
            const size_t num_queues_,
            const String & exchange_type_,
            const String & local_exchange_,
            const std::atomic<bool> & stopped_);

    ~ReadBufferFromRabbitMQConsumer() override;

    void allowNext() { allowed = true; } // Allow to read next message.
    void checkSubscription();
    void activateReading();

    auto getExchange() const { return exchange_name; }

private:
    const String & exchange_name;
    const Names & routing_keys;
    const size_t channel_id;
    const bool bind_by_id;
    const size_t num_queues;

    const String & exchange_type;
    const String & local_exchange;
    const String local_default_exchange;
    const String local_hash_exchange;

    Poco::Logger * log;
    char row_delimiter;
    bool stalled = false;
    bool allowed = true;
    const std::atomic<bool> & stopped;

    std::unique_ptr<uv_loop_t> loop;
    std::unique_ptr<RabbitMQHandler> event_handler;
    std::unique_ptr<AMQP::TcpConnection> connection;
    ChannelPtr consumer_channel;

    BackgroundSchedulePool::TaskHolder heartbeat_task;
    BackgroundSchedulePool::TaskHolder looping_task;
    bool loop_started = false;

    String default_local_exchange;
    bool local_exchange_declared = false, local_hash_exchange_declared = false;
    bool exchange_type_set = false, hash_exchange = false;

    std::atomic<bool> consumer_error = false;
    std::atomic<size_t> count_subscribed = 0, wait_subscribed;

    ConcurrentBoundedQueue<String> messages;
    String current;
    std::vector<String> queues;
    std::unordered_map<String, bool> subscribed_queue;

    bool nextImpl() override;

    void initExchange();
    void initQueueBindings(const size_t queue_id);
    void subscribe(const String & queue_name);
    void startEventLoop();
    void heartbeatFunc();
    void loopingFunc();

};
}
