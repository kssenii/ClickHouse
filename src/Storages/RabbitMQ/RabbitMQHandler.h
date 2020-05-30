#pragma once

#include <memory>
#include <amqpcpp.h>
#include <IO/ReadBuffer.h>
#include <IO/WriteBuffer.h>
#include <amqpcpp/libevent.h>
#include <amqpcpp/linux_tcp.h>
#include <Poco/Net/StreamSocket.h>
#include <common/types.h>
#include <event2/event.h>


namespace DB
{

class RabbitMQHandler : public AMQP::LibEventHandler
{
public:
    RabbitMQHandler(event_base * evbase_, Poco::Logger * log_, std::mutex & mutex_);
    RabbitMQHandler(event_base * evbase_, Poco::Logger * log_);

    void onError(AMQP::TcpConnection * connection, const char * message) override;

    void start();  /// this loop waits for active events and is stopped only after stop() method
    void start_producer();  /// this loop waits for active events and is stopped only after stop() method
    void startNonBlock();
    void stop();
    void free();

private:

    event_base * evbase;
    bool connection_error = false;
    std::mutex & mutex_ref;
    std::mutex mutex;

    Poco::Logger * log;
    String user_name;
    String password;
};

}
