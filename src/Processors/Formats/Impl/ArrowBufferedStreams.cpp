#ifdef HAS_RESERVED_IDENTIFIER
#pragma clang diagnostic ignored "-Wreserved-identifier"
#endif

#include "ArrowBufferedStreams.h"

#if USE_ARROW || USE_ORC || USE_PARQUET

#include <IO/ReadBufferFromFileDescriptor.h>
#include <IO/WriteBufferFromString.h>
#include <IO/copyData.h>
#include <arrow/buffer.h>
#include <arrow/io/memory.h>
#include <arrow/result.h>

#include <sys/stat.h>


namespace DB
{

ArrowBufferedOutputStream::ArrowBufferedOutputStream(WriteBuffer & out_) : out{out_}, is_open{true}
{
}

arrow::Status ArrowBufferedOutputStream::Close()
{
    is_open = false;
    return arrow::Status::OK();
}

arrow::Result<int64_t> ArrowBufferedOutputStream::Tell() const
{
    return arrow::Result<int64_t>(total_length);
}

arrow::Status ArrowBufferedOutputStream::Write(const void * data, int64_t length)
{
    out.write(reinterpret_cast<const char *>(data), length);
    total_length += length;
    return arrow::Status::OK();
}

RandomAccessFileFromSeekableReadBuffer::RandomAccessFileFromSeekableReadBuffer(SeekableReadBuffer & in_, off_t file_size_)
    : in{in_}, file_size{file_size_}, is_open{true}
{
    std::cerr << "================================= random access file from seekable read buffer ================================\n\n";
}

arrow::Result<int64_t> RandomAccessFileFromSeekableReadBuffer::GetSize()
{
    if (!file_size)
    {
        auto size = in.getTotalSizeToRead();
        file_size = *size;
        std::cerr << "\n\ngot file size: " << file_size << std::endl;
    }
    return arrow::Result<int64_t>(file_size);
}

arrow::Status RandomAccessFileFromSeekableReadBuffer::Close()
{
    is_open = false;
    return arrow::Status::OK();
}

arrow::Result<int64_t> RandomAccessFileFromSeekableReadBuffer::Tell() const
{
    return in.getPosition();
}

arrow::Result<int64_t> RandomAccessFileFromSeekableReadBuffer::Read(int64_t nbytes, void * out)
{
    return in.readBig(reinterpret_cast<char *>(out), nbytes);
}

arrow::Result<std::shared_ptr<arrow::Buffer>> RandomAccessFileFromSeekableReadBuffer::Read(int64_t nbytes)
{
    ARROW_ASSIGN_OR_RAISE(auto buffer, arrow::AllocateResizableBuffer(nbytes))
    ARROW_ASSIGN_OR_RAISE(int64_t bytes_read, Read(nbytes, buffer->mutable_data()))

    if (bytes_read < nbytes)
        RETURN_NOT_OK(buffer->Resize(bytes_read));

    return buffer;
}

arrow::Status RandomAccessFileFromSeekableReadBuffer::Seek(int64_t position)
{
    std::cerr << "\n\n\narrow inut stream from read buffer as arrow SEEK!!!!!!!\n\n";
    in.seek(position, SEEK_SET);
    return arrow::Status::OK();
}


ArrowInputStreamFromReadBuffer::ArrowInputStreamFromReadBuffer(ReadBuffer & in_) : in(in_), is_open{true}
{
    std::cerr << "\n\n\narrow inut stream from read buffer: " << typeid(in_).name() << std::endl;
}

arrow::Result<int64_t> ArrowInputStreamFromReadBuffer::Read(int64_t nbytes, void * out)
{
    std::cerr << "\n\n\narrow inut stream from read buffer read\n\n";
    return in.readBig(reinterpret_cast<char *>(out), nbytes);
}

arrow::Result<std::shared_ptr<arrow::Buffer>> ArrowInputStreamFromReadBuffer::Read(int64_t nbytes)
{
    ARROW_ASSIGN_OR_RAISE(auto buffer, arrow::AllocateResizableBuffer(nbytes))
    ARROW_ASSIGN_OR_RAISE(int64_t bytes_read, Read(nbytes, buffer->mutable_data()))

    if (bytes_read < nbytes)
        RETURN_NOT_OK(buffer->Resize(bytes_read));

    return buffer;
}

arrow::Status ArrowInputStreamFromReadBuffer::Abort()
{
    return arrow::Status();
}

arrow::Result<int64_t> ArrowInputStreamFromReadBuffer::Tell() const
{
    std::cerr << "\n\n\narrow inut stream from read buffer tell\n\n";
    return in.count();
}

arrow::Status ArrowInputStreamFromReadBuffer::Close()
{
    is_open = false;
    return arrow::Status();
}

std::shared_ptr<arrow::io::RandomAccessFile> asArrowFile(ReadBuffer & in)
{
    std::cerr << "\n\n\narrow inut stream from read buffer as arrow file: " << typeid(in).name() << std::endl;
    if (auto * fd_in = dynamic_cast<ReadBufferFromFileDescriptor *>(&in))
    {
        struct stat stat;
        auto res = ::fstat(fd_in->getFD(), &stat);
        // if fd is a regular file i.e. not stdin
        if (res == 0 && S_ISREG(stat.st_mode))
            return std::make_shared<RandomAccessFileFromSeekableReadBuffer>(*fd_in, stat.st_size);
    }
    else if (auto * seekable_read_buf = dynamic_cast<SeekableReadBuffer *>(&in))
    {
        std::cerr << "\n\nFILE IS SEEKABLE\n\n";
        /// Size is not passed, it will be set lazily.
        return std::make_shared<RandomAccessFileFromSeekableReadBuffer>(*seekable_read_buf, 0);
    }

    // fallback to loading the entire file in memory
    std::string file_data;
    {
        WriteBufferFromString file_buffer(file_data);
        copyData(in, file_buffer);
    }

    return std::make_shared<arrow::io::BufferReader>(arrow::Buffer::FromString(std::move(file_data)));
}

}

#endif
