#include <common/logger_useful.h>
#include <Storages/RabbitMQ/RabbitMQHandler.h>

namespace DB
{

static const auto Lock_timeout = 50;

RabbitMQHandler::RabbitMQHandler(uv_loop_t * loop_, Poco::Logger * log_) :
    AMQP::LibUvHandler(loop_),
    loop(loop_),
    log(log_)
{
}


void RabbitMQHandler::onError(AMQP::TcpConnection * connection, const char * message)
{
    LOG_ERROR(log, "Library error report: {}", message);

    if (!connection->usable() || !connection->ready())
    {
        LOG_ERROR(log, "Connection lost completely");
    }

    stop();
}


void RabbitMQHandler::startLoop()
{
    if (starting_loop.try_lock())
    {
        if (!stop_loop)
        {
            running_loop.store(true);
        }

        while (!stop_loop)
        {
            uv_run(loop, UV_RUN_NOWAIT);
        }

        running_loop.store(false);
        starting_loop.unlock();
    }
}


void RabbitMQHandler::startConsumerLoop(std::atomic<bool> & loop_started)
{
    /* The object of this class is shared between concurrent consumers (who share the same connection == share the same
     * event loop and handler). But the loop should not be attempted to start if it is already running.
     */
    std::lock_guard lock(mutex_before_event_loop);
    uv_run(loop, UV_RUN_NOWAIT);
}


void RabbitMQHandler::startProducerLoop()
{
    uv_run(loop, UV_RUN_NOWAIT);
}


void RabbitMQHandler::stop()
{
    //std::lock_guard lock(mutex_before_loop_stop);
    //uv_stop(loop);
    stop_loop = true;
}

}
