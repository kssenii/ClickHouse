#pragma once
#include <TableFunctions/ITableFunction.h>
#include <Storages/StorageMergeTreeParts.h>


namespace DB
{

class TableFunctionMergeTreeParts : public ITableFunction
{
public:
    static constexpr auto name = "mergeTreeParts";

    std::string getName() const override { return name; }

protected:
    StoragePtr executeImpl(
        const ASTPtr & ast_function,
        ContextPtr context,
        const std::string & table_name,
        ColumnsDescription cached_columns) const override;

    const char * getStorageTypeName() const override { return "MergeTreeParts"; }

    ColumnsDescription getActualTableStructure(ContextPtr context) const override;

    void parseArguments(const ASTPtr & ast_function, ContextPtr context) override;

private:
    std::string structure;

    StorageMergeTreeParts::Parts parts;
};

}
