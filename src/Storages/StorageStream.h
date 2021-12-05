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

    static void writeIntoStream(StorageStream & stream, const Block & block, ContextPtr context);

protected:
    StorageStream(
        const StorageID & table_id_,
        ContextPtr local_context,
        const ASTCreateQuery & query,
        const ColumnsDescription & columns_,
        bool attach_);

private:
    Subscriptions subscriptions;
    mutable std::mutex subscriptions_mutex;

    Poco::Logger * log;
    ASTPtr inner_query;

    StorageID select_table_id = StorageID::createEmpty();
};

}
