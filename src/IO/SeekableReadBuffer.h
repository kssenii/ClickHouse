#pragma once

#include <IO/ReadBuffer.h>

namespace DB
{

class ReadBufferWithKnownSize : public ReadBuffer
{
public:
    ReadBufferWithKnownSize(Position ptr, size_t size)
        : ReadBuffer(ptr, size) {}
    ReadBufferWithKnownSize(Position ptr, size_t size, size_t offset)
        : ReadBuffer(ptr, size, offset) {}

    virtual std::optional<size_t> getTotalSizeToRead() const = 0;
};

class SeekableReadBuffer : public ReadBufferWithKnownSize
{
public:
    SeekableReadBuffer(Position ptr, size_t size)
        : ReadBufferWithKnownSize(ptr, size) {}
    SeekableReadBuffer(Position ptr, size_t size, size_t offset)
        : ReadBufferWithKnownSize(ptr, size, offset) {}

    /**
     * Shifts buffer current position to given offset.
     * @param off Offset.
     * @param whence Seek mode (@see SEEK_SET, @see SEEK_CUR).
     * @return New position from the beginning of underlying buffer / file.
     */
    virtual off_t seek(off_t off, int whence) = 0;

    /**
     * Keep in mind that seekable buffer may encounter eof() once and the working buffer
     * may get into inconsistent state. Don't forget to reset it on the first nextImpl()
     * after seek().
     */

    /**
     * @return Offset from the begin of the underlying buffer / file corresponds to the buffer current position.
     */
    virtual off_t getPosition() = 0;

    std::optional<size_t> getTotalSizeToRead() const override { return std::nullopt; }
};

}
