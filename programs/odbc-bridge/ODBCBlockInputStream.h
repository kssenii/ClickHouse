#pragma once

#include <string>
#include <Core/Block.h>
#include <DataStreams/IBlockInputStream.h>
#include <Poco/Data/RecordSet.h>
#include <Poco/Data/Session.h>
#include <Poco/Data/Statement.h>
#include <Core/ExternalResultDescription.h>
#include <nanodbc/nanodbc.h>


namespace DB
{
/// Allows processing results of a query to ODBC source as a sequence of Blocks, simplifies chaining
class ODBCBlockInputStream final : public IBlockInputStream
{
public:
    using ConnectionPtr = std::shared_ptr<nanodbc::connection>;
    ODBCBlockInputStream(ConnectionPtr connection_, const std::string & query_str, const Block & sample_block, const UInt64 max_block_size_);

    String getName() const override { return "ODBC"; }

    Block getHeader() const override { return description.sample_block.cloneEmpty(); }

private:
    using QueryResult = std::shared_ptr<nanodbc::result>;
    using ValueType = ExternalResultDescription::ValueType;

    Block readImpl() override;

    void insertValue(IColumn & column, const ValueType type, nanodbc::result row, size_t idx);

    static void insertDefaultValue(IColumn & column, const IColumn & sample_column)
    {
        column.insertFrom(sample_column, 0);
    }

    Poco::Logger * log;
    const UInt64 max_block_size;
    ExternalResultDescription description;

    ConnectionPtr connection;
    nanodbc::result result;
    String query;
    bool finished = false;
};

}
