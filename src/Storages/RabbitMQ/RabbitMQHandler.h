#pragma once

#include <thread>
#include <memory>
#include <mutex>
#include <amqpcpp.h>
#include <amqpcpp/libevent.h>
#include <amqpcpp/linux_tcp.h>
#include <common/types.h>
#include <amqpcpp/libuv.h>

namespace DB
{

class RabbitMQHandler : public AMQP::LibUvHandler
{

public:
    RabbitMQHandler(uv_loop_t * evbase_, Poco::Logger * log_);

    void onError(AMQP::TcpConnection * connection, const char * message) override;
    void startConsumerLoop(std::atomic<bool> & loop_started);
    void startProducerLoop();
    void stopWithTimeout();
    void stop();

private:
    uv_loop_t * loop;
    Poco::Logger * log;

    timeval tv;
    size_t count_passed = 0;
    std::timed_mutex mutex_before_event_loop;
    std::timed_mutex mutex_before_loop_stop;
};

}
