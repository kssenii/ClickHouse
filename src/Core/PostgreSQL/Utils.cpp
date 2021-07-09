#include "Utils.h"
#include <IO/Operators.h>

namespace postgres
{

ConnectionInfo formatConnectionString(String dbname, String host, UInt16 port, String user, String password, size_t idle_connection_timeout)
{
    DB::WriteBufferFromOwnString out;
    out << "dbname=" << DB::quote << dbname
        << " host=" << DB::quote << host
        << " port=" << port
        << " user=" << DB::quote << user
        << " password=" << DB::quote << password
        << " connect_timeout=10"
        << " keepalives_idle=" << idle_connection_timeout;
    return std::make_pair(out.str(), host + ':' + DB::toString(port));
}

}
