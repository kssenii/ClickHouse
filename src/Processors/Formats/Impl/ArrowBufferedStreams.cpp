#ifdef HAS_RESERVED_IDENTIFIER
#pragma clang diagnostic ignored "-Wreserved-identifier"
#endif

#include "ArrowBufferedStreams.h"

#if USE_ARROW || USE_ORC || USE_PARQUET

#include <Common/assert_cast.h>
#include <IO/ReadBufferFromFileDescriptor.h>
#include <IO/WriteBufferFromString.h>
#include <IO/copyData.h>
#include <Formats/FormatSettings.h>
#include <arrow/buffer.h>
#include <arrow/io/memory.h>
#include <arrow/result.h>
// #include <arrow/filesystem/s3_internal.h>
#include <aws/s3/S3Client.h>

#include <arrow/util/key_value_metadata.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <aws/s3/model/HeadObjectRequest.h>
#include <Disks/IO/ReadBufferFromS3.h>
#include <sys/stat.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int S3_ERROR;
}

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
}

RandomAccessFileFromSeekableReadBuffer::RandomAccessFileFromSeekableReadBuffer(SeekableReadBufferWithSize & in_)
    : in{in_}, is_open{true}
{
}

arrow::Result<int64_t> RandomAccessFileFromSeekableReadBuffer::GetSize()
{
    if (!file_size)
    {
        auto * buf_with_size = assert_cast<SeekableReadBufferWithSize *>(&in);
        file_size = buf_with_size->getTotalSize();
    }
    return arrow::Result<int64_t>(*file_size);
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
    in.seek(position, SEEK_SET);
    return arrow::Status::OK();
}


ArrowInputStreamFromReadBuffer::ArrowInputStreamFromReadBuffer(ReadBuffer & in_) : in(in_), is_open{true}
{
}

arrow::Result<int64_t> ArrowInputStreamFromReadBuffer::Read(int64_t nbytes, void * out)
{
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
    return in.count();
}

arrow::Status ArrowInputStreamFromReadBuffer::Close()
{
    is_open = false;
    return arrow::Status();
}


// A RandomAccessFile that reads from a S3 object
class ObjectInputFile final : public arrow::io::RandomAccessFile {
public:
    ObjectInputFile(
            SeekableReadBuffer & in_,
            std::shared_ptr<Aws::S3::S3Client> client_,
            const String & bucket_,
            const String & key_,
            const arrow::io::IOContext& io_context_ = arrow::io::default_io_context())
        : in(in_)
        , client(std::move(client_))
        , io_context(io_context_)
        , bucket(bucket_)
        , key(key_)
    {
        auto status = init();
        if (!status.ok())
            throw Exception(ErrorCodes::S3_ERROR, status.message());
    }

    template <typename ObjectResult> std::shared_ptr<const arrow::KeyValueMetadata>
    getObjectMetadata(const ObjectResult& result)
    {
        auto md = std::make_shared<arrow::KeyValueMetadata>();
        auto push = [&](std::string k, const Aws::String& v)
        {
            if (!v.empty())
                md->Append(std::move(k), std::string(v.data(), v.length()));
        };
        auto push_datetime = [&](std::string k, const Aws::Utils::DateTime & v) {
        if (v != Aws::Utils::DateTime(0.0))
            push(std::move(k), v.ToGmtString(Aws::Utils::DateFormat::ISO_8601));
        };

        md->Append("Content-Length", std::to_string(result.GetContentLength()));
        push("Cache-Control", result.GetCacheControl());
        push("Content-Type", result.GetContentType());
        push("Content-Language", result.GetContentLanguage());
        push("ETag", result.GetETag());
        push("VersionId", result.GetVersionId());
        push_datetime("Last-Modified", result.GetLastModified());
        push_datetime("Expires", result.GetExpires());
        return md;
    }

    arrow::Status init()
    {
        Aws::S3::Model::HeadObjectRequest req;
        req.SetBucket(bucket);
        req.SetKey(key);

        auto outcome = client->HeadObject(req);
        if (!outcome.IsSuccess())
            throw Exception(ErrorCodes::S3_ERROR, outcome.GetError().GetMessage());

        content_length = outcome.GetResult().GetContentLength();
        metadata = getObjectMetadata(outcome.GetResult());
        return arrow::Status::OK();
    }

    // RandomAccessFile APIs

    arrow::Result<std::shared_ptr<const arrow::KeyValueMetadata>> ReadMetadata() override
    {
        return metadata;
    }

   //  arrow::Future<std::shared_ptr<const arrow::KeyValueMetadata>>
   //  ReadMetadataAsync(const arrow::io::IOContext&) override
   //  {
   //      return metadata;
   //  }

    arrow::Status Close() override {
      client = nullptr;
      is_closed = true;
      return arrow::Status::OK();
    }

    bool closed() const override { return is_closed; }

    arrow::Result<int64_t> Tell() const override
    {
        return pos;
    }

    arrow::Result<int64_t> GetSize() override
    {
        return content_length.value();
    }

    arrow::Status Seek(int64_t position) override
    {
        pos = position;
        return arrow::Status::OK();
    }

    arrow::Result<int64_t> ReadAt(int64_t position, int64_t nbytes, void* out) override
    {
        nbytes = std::min(nbytes, content_length.value() - position);
        if (nbytes == 0)
            return 0;

        if (in.getPosition() <= position)
        {
            size_t diff = position - in.getPosition();
            if (diff <= DBMS_DEFAULT_BUFFER_SIZE * 10)
                in.ignore(diff);
            else
            {

                std::cerr << "\n\nSeek to " << position << " to read " << nbytes << " bytes\n";
                in.seek(position, SEEK_SET);
            }
        }
        else
        {
            // in.setReadUntilPosition(nbytes);
            std::cerr << "\n\nSeek (2) to " << position << " to read " << nbytes << " bytes\n";
            // in.setReadUntilPosition(position + nbytes - 1);
            in.seek(position, SEEK_SET);
            // in.setReadUntilEnd();
        }

        auto bytes_read = in.readBig(reinterpret_cast<char *>(out), nbytes);
        pos += bytes_read;
        return bytes_read;
    }

    arrow::Result<std::shared_ptr<arrow::Buffer>> ReadAt(int64_t position, int64_t nbytes) override
    {
        ARROW_ASSIGN_OR_RAISE(auto buffer, arrow::AllocateResizableBuffer(nbytes))
        ARROW_ASSIGN_OR_RAISE(int64_t bytes_read, ReadAt(position, nbytes, buffer->mutable_data()))

        if (bytes_read < nbytes)
            RETURN_NOT_OK(buffer->Resize(bytes_read));

        return std::move(buffer);
    }

    arrow::Result<int64_t> Read(int64_t nbytes, void* out) override
    {
        return ReadAt(pos, nbytes, out);
    }

    arrow::Result<std::shared_ptr<arrow::Buffer>> Read(int64_t nbytes) override
    {
        return ReadAt(pos, nbytes);
    }

protected:
    SeekableReadBuffer & in;

    std::shared_ptr<Aws::S3::S3Client> client;
    const arrow::io::IOContext io_context;
    String bucket;
    String key;

    std::optional<int64_t> content_length;
    std::shared_ptr<const arrow::KeyValueMetadata> metadata;

    bool is_closed = false;
    int64_t pos = 0;
};


std::shared_ptr<arrow::io::RandomAccessFile> asArrowFile(ReadBuffer & in, const FormatSettings & settings)
{
    if (auto * fd_in = dynamic_cast<ReadBufferFromFileDescriptor *>(&in))
    {
        struct stat stat;
        auto res = ::fstat(fd_in->getFD(), &stat);
        // if fd is a regular file i.e. not stdin
        if (res == 0 && S_ISREG(stat.st_mode))
            return std::make_shared<RandomAccessFileFromSeekableReadBuffer>(*fd_in, stat.st_size);
    }
    else if (auto * seekable_in = dynamic_cast<SeekableReadBufferWithSize *>(&in))
    {
        if (settings.seekable_read)
        {
            // if (auto * s3_in = dynamic_cast<ReadBufferFromS3 *>(&in))
            // {
            //     return std::make_shared<ObjectInputFile>(*s3_in, s3_in->client_ptr, s3_in->bucket, s3_in->key);
            // }
            // else
            {
                return std::make_shared<RandomAccessFileFromSeekableReadBuffer>(*seekable_in);
            }
        }
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
