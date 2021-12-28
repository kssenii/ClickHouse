#include <Storages/IStorage.h>

namespace Poco { class Logger; }

namespace DB
{

class StorageStream final : public shared_ptr_helper<StorageStream>, public IStorage, WithMutableContext
{
    friend struct shared_ptr_helper<StorageStream>;

public:
    using Subscriptions = std::set<StorageID>;

    std::string getName() const override { return "Stream"; }

    bool isStream() const override { return true; }

    void startup() override;
    void shutdown() override;

    void subscribe(const StorageID & subscriber, bool if_not_subscribed) override;
    void unsubscribe(const StorageID & subscriber, bool if_subscribed) override;

    static Subscriptions getSubscriptions(const StorageStream & stream);

    ASTPtr getInnerQuery() const { return inner_query; }

    void writeIntoStream(const Block & block, ContextPtr context);

    void writeIntoStorage(const StorageID & target_storage_id, Pipe pipe, ContextPtr local_context);

protected:
    StorageStream(
        const StorageID & table_id_,
        ContextPtr local_context,
        const ASTCreateQuery & query,
        const ColumnsDescription & columns_,
        bool attach_);

private:
    enum class FlushStrategy
    {
        DEFAULT, /// Just flush each new block at once
        INNER_QUERY_RESULT_UPDATE, /// same as live view, flush when result of the query changes.
        TIME_WINDOW, /// same as window view.
    };

    FlushStrategy strategy = FlushStrategy::DEFAULT;

    Subscriptions subscriptions;
    mutable std::mutex subscriptions_mutex;

    Poco::Logger * log;
    ASTPtr inner_query;

    StorageID select_table_id = StorageID::createEmpty();
};

}
