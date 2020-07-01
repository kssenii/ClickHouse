#include <common/logger_useful.h>
#include <Common/Exception.h>
#include <Storages/RabbitMQ/RabbitMQHandler.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_CONNECT_RABBITMQ;
}

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
        throw Exception("Connection error", ErrorCodes::CANNOT_CONNECT_RABBITMQ);
    }
}


void RabbitMQHandler::startBackgroundLoop()
{
    /// stop_loop variable is updated in a separate thread
    while (!stop_loop.load())
    {
        uv_run(loop, UV_RUN_NOWAIT);
    }
}


void RabbitMQHandler::startLoop()
{
    uv_run(loop, UV_RUN_NOWAIT);
}

}
