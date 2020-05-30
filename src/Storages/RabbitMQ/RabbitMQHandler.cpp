#include <Poco/Net/StreamSocket.h>
#include <Storages/RabbitMQ/RabbitMQHandler.h>
#include <common/logger_useful.h>


namespace DB
{

RabbitMQHandler::RabbitMQHandler(event_base * evbase_, Poco::Logger * log_, std::mutex & mutex_) :
    LibEventHandler(evbase_),
    evbase(evbase_),
    log(log_),
    mutex_ref(mutex_)
{
}


void RabbitMQHandler::onError(AMQP::TcpConnection * /*connection*/, const char * message) 
{
    LOG_ERROR(log, "Library error report: " << message);
    stop();
}


void RabbitMQHandler::start()
{
    std::lock_guard lock(mutex);
    event_base_dispatch(evbase);
}


void RabbitMQHandler::start_producer()
{
    std::lock_guard lock(mutex_ref);
    event_base_loop(evbase, EVLOOP_NONBLOCK); 
}



void RabbitMQHandler::startNonBlock()
{
    std::lock_guard lock(mutex);
    event_base_loop(evbase, EVLOOP_NONBLOCK); 
}


void RabbitMQHandler::stop()
{
    event_base_loopbreak(evbase);
}


void RabbitMQHandler::free()
{
    event_base_free(evbase);
}

}
