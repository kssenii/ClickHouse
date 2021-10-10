#include "AsynchronousReadIndirectBufferFromRemoteFS.h"

#include <Common/Stopwatch.h>
#include <IO/ThreadPoolRemoteFSReader.h>


namespace CurrentMetrics
{
    extern const Metric AsynchronousReadWait;
}
namespace ProfileEvents
{
    extern const Event AsynchronousReadWaitMicroseconds;
    extern const Event RemoteFSSeeks;
    extern const Event RemoteFSPrefetches;
    extern const Event RemoteFSPrefetchRead;
    extern const Event RemoteFSAsyncBufferReads;
    extern const Event RemoteFSSeekCancelledPrefetches;
    extern const Event RemoteFSDesCancelledPrefetches;
    extern const Event RemoteFSAsyncBuffers;
    extern const Event RemoteFSSeek1;
    extern const Event RemoteFSSeek2;
    extern const Event RemoteFSSeek3;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_SEEK_THROUGH_FILE;
}

static std::atomic<size_t> prio = 0;

AsynchronousReadIndirectBufferFromRemoteFS::AsynchronousReadIndirectBufferFromRemoteFS(
    AsynchronousReaderPtr reader_, Int32 priority_,
    std::shared_ptr<ReadBufferFromRemoteFSGather> impl_, size_t buf_size_)
    : ReadBufferFromFileBase(buf_size_, nullptr, 0)
    , reader(reader_)
    , priority(priority_)
    , impl(impl_)
    , prefetch_buffer(buf_size_)
{
    ProfileEvents::increment(ProfileEvents::RemoteFSAsyncBuffers);
}


std::future<IAsynchronousReader::Result> AsynchronousReadIndirectBufferFromRemoteFS::readInto(char * data, size_t size)
{
    IAsynchronousReader::Request request;
    request.descriptor = std::make_shared<ThreadPoolRemoteFSReader::RemoteFSFileDescriptor>(impl);
    request.buf = data;
    request.size = size;
    request.offset = absolute_position;
    auto p = prio.fetch_add(1);
    if (priority) {}
    request.priority = p;
    return reader->submit(request);
}


void AsynchronousReadIndirectBufferFromRemoteFS::prefetch()
{
    if (hasPendingData())
        return;

    if (prefetch_future.valid())
        return;

    ++prefetch_count;
    ProfileEvents::increment(ProfileEvents::RemoteFSPrefetches);
    prefetch_future = readInto(prefetch_buffer.data(), prefetch_buffer.size());
    events += "-- Prefetch --";
}


bool AsynchronousReadIndirectBufferFromRemoteFS::nextImpl()
{
    size_t size = 0;
    ProfileEvents::increment(ProfileEvents::RemoteFSAsyncBufferReads);

    reads += 1;
    if (prefetch_future.valid())
    {
        events += "-- Read from prefetch --";

        read_unprefetched = false;
        ProfileEvents::increment(ProfileEvents::RemoteFSPrefetchRead);
        CurrentMetrics::Increment metric_increment{CurrentMetrics::AsynchronousReadWait};
        Stopwatch watch;
        {
            size = prefetch_future.get();
            if (size)
            {
                memory.swap(prefetch_buffer);
                set(memory.data(), memory.size());
                working_buffer.resize(size);
                absolute_position += size;
            }
        }
        watch.stop();
        ProfileEvents::increment(ProfileEvents::AsynchronousReadWaitMicroseconds, watch.elapsedMicroseconds());

        prefetch_future = {};
    }
    else
    {
        events += "-- Read without prefetch --";

        read_unprefetched = true;
        size = impl->readInto(memory.data(), memory.size(), absolute_position);
        if (size)
        {
            set(memory.data(), memory.size());
            working_buffer.resize(size);
            absolute_position += size;
        }

        prefetch_future = {};
    }

    events += " + " + toString(size) + " + ";

    // if (need_to_read)
    // {
    //     std::cerr << "need to prefetch: " << need_to_read << " - " << size << std::endl;
    //     if (need_to_prefetch > size)
    //     need_to_read -= size;
    //     if (need_to_read)
    //     {
    //         ++prefetch_count;
    //         ProfileEvents::increment(ProfileEvents::RemoteFSPrefetches);
    //         prefetch_future = readInto(prefetch_buffer.data(), prefetch_buffer.size());
    //         events += "-- Dop Prefetch --";
    //     }
    // }

    std::cerr << "\n\nEvents: " << events << std::endl;
    return size;
}


off_t AsynchronousReadIndirectBufferFromRemoteFS::seek(off_t offset_, int whence)
{
    ProfileEvents::increment(ProfileEvents::RemoteFSSeeks);
    events += "-- Seek to " + toString(offset_) + " --";
    if (whence == SEEK_CUR)
    {
        /// If position within current working buffer - shift pos.
        if (!working_buffer.empty() && static_cast<size_t>(getPosition() + offset_) < absolute_position)
        {
            pos += offset_;
            ProfileEvents::increment(ProfileEvents::RemoteFSSeek1);
            return getPosition();
        }
        else
        {
            absolute_position += offset_;
        }
    }
    else if (whence == SEEK_SET)
    {
        /// If position is within current working buffer - shift pos.
        if (!working_buffer.empty()
            && static_cast<size_t>(offset_) >= absolute_position - working_buffer.size()
            && size_t(offset_) < absolute_position)
        {
            pos = working_buffer.end() - (absolute_position - offset_);

            assert(pos >= working_buffer.begin());
            assert(pos <= working_buffer.end());

            ProfileEvents::increment(ProfileEvents::RemoteFSSeek2);
            return getPosition();
        }
        else
        {
            absolute_position = offset_;
        }
    }
    else
        throw Exception("Only SEEK_SET or SEEK_CUR modes are allowed.", ErrorCodes::CANNOT_SEEK_THROUGH_FILE);

    if (prefetch_future.valid())
    {
        std::cerr << "Prefetch is valid. Cancelling\n";
        ProfileEvents::increment(ProfileEvents::RemoteFSSeekCancelledPrefetches);
        prefetch_future.wait();
        prefetch_future = {};
    }

    ProfileEvents::increment(ProfileEvents::RemoteFSSeek3);
    pos = working_buffer.end();
    impl->reset();

    return absolute_position;
}


void AsynchronousReadIndirectBufferFromRemoteFS::finalize()
{
    std::cerr << "\n\n\nFinalize. Prefetches: " << prefetch_count << ", reads: " << reads << ", file: " << getFileName() << "\n\n\n\n";
    std::cerr << "Events: " << events << "\n\n";
    if (prefetch_future.valid())
    {
        std::cerr << "Prefetch is valid. Waiting\n";
        ProfileEvents::increment(ProfileEvents::RemoteFSDesCancelledPrefetches);
        prefetch_future.wait();
        prefetch_future = {};
    }
}


AsynchronousReadIndirectBufferFromRemoteFS::~AsynchronousReadIndirectBufferFromRemoteFS()
{
    finalize();
}

}
