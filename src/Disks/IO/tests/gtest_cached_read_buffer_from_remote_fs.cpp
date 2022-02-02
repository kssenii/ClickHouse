#include <Common/tests/gtest_global_context.h>
#include <Common/tests/lru_file_cache_test_helpers.h>

#include <Common/CurrentThread.h>
#include <Interpreters/Context.h>

#include <Common/getRandomASCIIString.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadBufferFromS3.h>
#include <IO/WriteBufferFromS3.h>
#include <IO/ReadBufferFromFile.h>
#include <Disks/IO/CachedReadBufferFromRemoteFS.h>

#include <IO/S3Common.h>
#include <Storages/StorageS3Settings.h>
#include <aws/s3/S3Client.h>
#include <aws/core/client/DefaultRetryStrategy.h>

static String cache_base_path = fs::current_path() / "test_cached_read_buffer" / "";

std::shared_ptr<Aws::S3::S3Client> getS3Client(const DB::S3::URI & uri)
{
    DB::S3::PocoHTTPClientConfiguration client_configuration = DB::S3::ClientFactory::instance().createClientConfiguration(
        /* region */"",
        getContext().context->getRemoteHostFilter(),
        getContext().context->getGlobalContext()->getSettingsRef().s3_max_redirects);
    client_configuration.connectTimeoutMs = 10000;
    client_configuration.requestTimeoutMs = 5000;
    client_configuration.maxConnections = 100;
    client_configuration.endpointOverride = uri.endpoint;
    client_configuration.retryStrategy = std::make_shared<Aws::Client::DefaultRetryStrategy>(10);

    return DB::S3::ClientFactory::instance().create(
        client_configuration, uri.is_virtual_hosted_style,
        /* access_key_id */"test", /* secret_access_key */"testtest",
        "", {}, false, false);
}

TEST(RemoteFSCache, main)
{
    if (fs::exists(cache_base_path))
        fs::remove_all(cache_base_path);
    fs::create_directory(cache_base_path);

    DB::ThreadStatus thread_status;

    /// To work with cache need query_id and query context.
    auto query_context = DB::Context::createCopy(getContext().context);
    query_context->makeQueryContext();
    query_context->setCurrentQueryId("query_id");
    DB::CurrentThread::QueryScope query_scope_holder(query_context);

    size_t buffer_size = 10;
    auto cache = std::make_shared<DB::LRUFileCache>(cache_base_path, buffer_size * 4, 1000);

    DB::S3::URI uri(Poco::URI("http://localhost:11111/test/"));
    auto client = getS3Client(uri);

    String remote_file_path = "test";
    auto path_key = cache->hash(remote_file_path);

    size_t file_size = buffer_size * 10;
    {
        DB::WriteBufferFromS3 buf(client, uri.bucket, remote_file_path,
                                  getContext().context->getSettingsRef().s3_min_upload_part_size,
                                  getContext().context->getSettingsRef().s3_min_upload_part_size);

        auto data = DB::getRandomASCIIString(file_size);
        buf.write(data.data(), data.size());
    }

    DB::ReadSettings read_settings;
    read_settings.remote_fs_buffer_size = buffer_size;

    size_t read_until_position = buffer_size * 8;

    auto remote_file_reader_creator = [=]()
    {
        return std::make_unique<DB::ReadBufferFromS3>(
            client, uri.bucket, remote_file_path, /* max_single_read_tries */5,
            read_settings, /* use_external_buffer */false, /* read_until_position */read_until_position, true);
    };

    {
        auto cached = std::make_shared<DB::CachedReadBufferFromRemoteFS>(
            remote_file_path, cache, remote_file_reader_creator, read_settings, read_until_position);

        cached->next();
        ASSERT_TRUE(cached->buffer().size() == buffer_size);
        auto result = std::string(cached->buffer().begin(), buffer_size);

        DB::ReadBufferFromFile cache_file_0(cache->path(path_key, 0));
        String expected;
        DB::readString(expected, cache_file_0);

        ASSERT_EQ(result, expected);
    }

    {
        auto cached = std::make_shared<DB::CachedReadBufferFromRemoteFS>(
            remote_file_path, cache, remote_file_reader_creator, read_settings, read_until_position);

        cached->seek(5, SEEK_SET);

        String result(buffer_size, '0');
        cached->read(result.data(), buffer_size);

        DB::ReadBufferFromFile cache_file_0(cache->path(path_key, 0));
        cache_file_0.seek(5, SEEK_SET);
        String expected_part_1;
        DB::readString(expected_part_1, cache_file_0);

        DB::ReadBufferFromFile cache_file_10(cache->path(path_key, 10));
        String expected_part_10;
        DB::readString(expected_part_10, cache_file_10);
        auto expected = expected_part_1 + expected_part_10;
        expected.resize(buffer_size);

        ASSERT_EQ(result, expected);
    }
}
