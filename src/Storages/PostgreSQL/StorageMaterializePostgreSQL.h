#pragma once

#include "config_core.h"
#include <ext/shared_ptr_helper.h>
#include <Interpreters/Context.h>
#include <Storages/IStorage.h>
#include "pqxx/pqxx"

namespace DB
{

class StorageMaterializePostgreSQL final : public ext::shared_ptr_helper<StorageMaterializePostgreSQL>, public IStorage
{
    friend struct ext::shared_ptr_helper<StorageMaterializePostgreSQL>;

public:
    StorageMaterializePostgreSQL(
        const StorageID & table_id_,
        const String & remote_table_name_,
        const ColumnsDescription & columns_,
        const ConstraintsDescription & constraints_,
        const Context & context_);

    String getName() const override { return "MaterializePostgreSQL"; }

private:
    String remote_table_name;
    Context global_context;
};

}

