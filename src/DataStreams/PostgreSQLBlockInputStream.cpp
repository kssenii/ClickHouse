#include "PostgreSQLBlockInputStream.h"

#if USE_LIBPQXX
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnDecimal.h>
#include <DataTypes/IDataType.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypesDecimal.h>
#include <Interpreters/convertFieldToType.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadBufferFromString.h>
#include <Common/assert_cast.h>
#include <ext/range.h>
#include <common/logger_useful.h>


namespace DB
{


template<typename T>
PostgreSQLBlockInputStream<T>::PostgreSQLBlockInputStream(
    std::shared_ptr<T> tx_,
    const std::string & query_str_,
    const Block & sample_block,
    const UInt64 max_block_size_,
    bool auto_commit_)
    : query_str(query_str_)
    , max_block_size(max_block_size_)
    , auto_commit(auto_commit_)
    , tx(tx_)
{
    description.init(sample_block);
}


template<typename T>
void PostgreSQLBlockInputStream<T>::readPrefix()
{
    for (const auto idx : ext::range(0, description.sample_block.columns()))
        if (description.types[idx].first == ExternalResultDescription::ValueType::vtArray)
            preparePostgreSQLArrayInfo(array_info, idx, description.sample_block.getByPosition(idx).type);
    /// pqxx::stream_from uses COPY command, will get error if ';' is present
    if (query_str.ends_with(';'))
        query_str.resize(query_str.size() - 1);

    stream = std::make_unique<pqxx::stream_from>(*tx, pqxx::from_query, std::string_view(query_str));
}


template<typename T>
Block PostgreSQLBlockInputStream<T>::readImpl()
{
    /// Check if pqxx::stream_from is finished
    if (!stream || !(*stream))
        return Block();

    MutableColumns columns = description.sample_block.cloneEmptyColumns();
    size_t num_rows = 0;

    while (true)
    {
        const std::vector<pqxx::zview> * row{stream->read_row()};

        /// row is nullptr if pqxx::stream_from is finished
        if (!row)
            break;

        for (const auto idx : ext::range(0, row->size()))
        {
            const auto & sample = description.sample_block.getByPosition(idx);

            /// if got NULL type, then pqxx::zview will return nullptr in c_str()
            if ((*row)[idx].c_str())
            {
                if (description.types[idx].second)
                {
                    ColumnNullable & column_nullable = assert_cast<ColumnNullable &>(*columns[idx]);
                    const auto & data_type = assert_cast<const DataTypeNullable &>(*sample.type);

                    insertPostgreSQLValue(
                            column_nullable.getNestedColumn(), (*row)[idx],
                            description.types[idx].first, data_type.getNestedType(), array_info, idx);

                    column_nullable.getNullMapData().emplace_back(0);
                }
                else
                {
                    insertPostgreSQLValue(
                            *columns[idx], (*row)[idx], description.types[idx].first, sample.type, array_info, idx);
                }
            }
            else
            {
                insertDefaultPostgreSQLValue(*columns[idx], *sample.column);
            }

        }

        if (++num_rows == max_block_size)
            break;
    }

    return description.sample_block.cloneWithColumns(std::move(columns));
}


template<typename T>
void PostgreSQLBlockInputStream<T>::readSuffix()
{
    if (stream)
    {
        stream->complete();

<<<<<<< HEAD
        if (auto_commit)
            tx->commit();
=======
void PostgreSQLBlockInputStream::insertValue(IColumn & column, std::string_view value,
        const ExternalResultDescription::ValueType type, const DataTypePtr data_type, size_t idx)
{
    switch (type)
    {
        case ValueType::vtUInt8:
            assert_cast<ColumnUInt8 &>(column).insertValue(pqxx::from_string<uint16_t>(value));
            break;
        case ValueType::vtUInt16:
            assert_cast<ColumnUInt16 &>(column).insertValue(pqxx::from_string<uint16_t>(value));
            break;
        case ValueType::vtUInt32:
            assert_cast<ColumnUInt32 &>(column).insertValue(pqxx::from_string<uint32_t>(value));
            break;
        case ValueType::vtUInt64:
            assert_cast<ColumnUInt64 &>(column).insertValue(pqxx::from_string<uint64_t>(value));
            break;
        case ValueType::vtInt8:
            assert_cast<ColumnInt8 &>(column).insertValue(pqxx::from_string<int16_t>(value));
            break;
        case ValueType::vtInt16:
            assert_cast<ColumnInt16 &>(column).insertValue(pqxx::from_string<int16_t>(value));
            break;
        case ValueType::vtInt32:
            assert_cast<ColumnInt32 &>(column).insertValue(pqxx::from_string<int32_t>(value));
            break;
        case ValueType::vtInt64:
            assert_cast<ColumnInt64 &>(column).insertValue(pqxx::from_string<int64_t>(value));
            break;
        case ValueType::vtFloat32:
            assert_cast<ColumnFloat32 &>(column).insertValue(pqxx::from_string<float>(value));
            break;
        case ValueType::vtFloat64:
            assert_cast<ColumnFloat64 &>(column).insertValue(pqxx::from_string<double>(value));
            break;
        case ValueType::vtFixedString:[[fallthrough]];
        case ValueType::vtString:
            assert_cast<ColumnString &>(column).insertData(value.data(), value.size());
            break;
        case ValueType::vtUUID:
            assert_cast<ColumnUInt128 &>(column).insert(parse<UUID>(value.data(), value.size()));
            break;
        case ValueType::vtDate:
            assert_cast<ColumnUInt16 &>(column).insertValue(UInt16{LocalDate{std::string(value)}.getDayNum()});
            break;
        case ValueType::vtDateTime:
        {
            ReadBufferFromString in(value);
            time_t time = 0;
            readDateTimeText(time, in);
            if (time < 0)
                time = 0;
            assert_cast<ColumnUInt32 &>(column).insertValue(time);
            break;
        }
        case ValueType::vtDateTime64:[[fallthrough]];
        case ValueType::vtDecimal32: [[fallthrough]];
        case ValueType::vtDecimal64: [[fallthrough]];
        case ValueType::vtDecimal128: [[fallthrough]];
        case ValueType::vtDecimal256:
        {
            ReadBufferFromString istr(value);
            data_type->deserializeAsWholeText(column, istr, FormatSettings{});
            break;
        }
        case ValueType::vtArray:
        {
            pqxx::array_parser parser{value};
            std::pair<pqxx::array_parser::juncture, std::string> parsed = parser.get_next();

            size_t dimension = 0, max_dimension = 0, expected_dimensions = array_info[idx].num_dimensions;
            const auto parse_value = array_info[idx].pqxx_parser;
            std::vector<std::vector<Field>> dimensions(expected_dimensions + 1);

            while (parsed.first != pqxx::array_parser::juncture::done)
            {
                if ((parsed.first == pqxx::array_parser::juncture::row_start) && (++dimension > expected_dimensions))
                    throw Exception("Got more dimensions than expected", ErrorCodes::BAD_ARGUMENTS);

                else if (parsed.first == pqxx::array_parser::juncture::string_value)
                    dimensions[dimension].emplace_back(parse_value(parsed.second));

                else if (parsed.first == pqxx::array_parser::juncture::null_value)
                    dimensions[dimension].emplace_back(array_info[idx].default_value);

                else if (parsed.first == pqxx::array_parser::juncture::row_end)
                {
                    max_dimension = std::max(max_dimension, dimension);

                    if (--dimension == 0)
                        break;

                    dimensions[dimension].emplace_back(Array(dimensions[dimension + 1].begin(), dimensions[dimension + 1].end()));
                    dimensions[dimension + 1].clear();
                }

                parsed = parser.get_next();
            }

            if (max_dimension < expected_dimensions)
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                        "Got less dimensions than expected. ({} instead of {})", max_dimension, expected_dimensions);

            assert_cast<ColumnArray &>(column).insert(Array(dimensions[1].begin(), dimensions[1].end()));
            break;
        }
    }
}

template
class PostgreSQLBlockInputStream<pqxx::work>;

template
class PostgreSQLBlockInputStream<pqxx::read_transaction>;

}

#endif
