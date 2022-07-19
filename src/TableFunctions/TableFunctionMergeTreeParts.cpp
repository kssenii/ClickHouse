#include <TableFunctions/TableFunctionMergeTreeParts.h>

#include <Common/assert_cast.h>
#include <Parsers/ASTFunction.h>
#include <Storages/checkAndGetLiteralArgument.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <TableFunctions/parseColumnsListForTableFunction.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

StoragePtr TableFunctionMergeTreeParts::executeImpl(
    const ASTPtr & /*ast_function*/, ContextPtr context, const String & table_name, ColumnsDescription /*cached_columns*/) const
{
    auto columns = getActualTableStructure(context);

    auto storage = std::make_shared<StorageMergeTreeParts>(
        parts,
        StorageID(getDatabaseName(), table_name),
        columns,
        ConstraintsDescription{},
        context);

    storage->startup();
    return storage;
}

ColumnsDescription TableFunctionMergeTreeParts::getActualTableStructure(ContextPtr context) const
{
    return parseColumnsListFromString(structure, context);
}

void TableFunctionMergeTreeParts::parseArguments(const ASTPtr & ast_function, ContextPtr)
{
    static constexpr auto arguments_num = 3;
    static const auto help_message = fmt::format(
        "Table function `{}` requires {} arguments: \n"
        "1) Column names and theis types which will be read: `structure(x Int8, y String, ...)`; "
        "2) Data parts information represented as a lite",
        getName(), arguments_num);

    const auto & func_args = ast_function->as<ASTFunction &>();

    if (!func_args.arguments)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Table function `{}` must have arguments", getName());

    ASTs & args = func_args.arguments->children;

    if (args.size() != arguments_num)
        throw Exception(
            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
            "Incorrect number of arguments {}. {}",
            args.size(), help_message);

    auto throw_bad_arguement = [](size_t arg_num, const std::string & hint = "")
    {
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Unexpected value for argument #{}{}. {}",
            arg_num, hint.empty() ? hint : ": " + hint, help_message);
    };

    /// Parse structure as `structure(x Int8, y String, ...)`.
    {
        const auto * structure_function = args[0]->as<ASTFunction>();
        if (!structure_function || structure_function->name != "structure")
            throw_bad_arguement(0);

        const auto * structure_function_args_expr = assert_cast<const ASTExpressionList *>(structure_function->arguments.get());
        if (!structure_function_args_expr)
            throw_bad_arguement(0, "expected list of expressions");

        const auto & structure_function_args = structure_function_args_expr->children;
        if (structure_function_args.size() != 1)
            throw_bad_arguement(0, "expected single argument");

        structure = checkAndGetLiteralArgument<String>(structure_function_args[0], "structure");
    }

    /// Parse data parts information:
    /// parts
    /// (
    ///     Wide(files('', '', ...), range=(x, y))
    ///     ...
    /// )
    {
        const auto * parts_function = args[1]->as<ASTFunction>();
        if (!parts_function || parts_function->name != "parts")
            throw_bad_arguement(1);

        const auto * parts_function_args_expr = assert_cast<const ASTExpressionList *>(parts_function->arguments.get());
        if (!parts_function_args_expr)
            throw_bad_arguement(1);

        const auto & parts_function_args = parts_function_args_expr->children;
        if (parts_function_args.empty())
            throw_bad_arguement(1, "expected non-zero number of arguments");

        for (const auto & part_expr : parts_function_args)
        {
            StorageMergeTreeParts::PartInfo part;

            const auto * part_function = part_expr->as<ASTFunction>();

            if (!part_function)
                throw_bad_arguement(1, "expected list of `part_type(...)` as `parts(...)` argument");

            part.type.fromString(part_function->name);

            const auto * part_function_args_expr = assert_cast<const ASTExpressionList *>(part_function->arguments.get());
            if (!part_function_args_expr)
                throw_bad_arguement(1, "expected list of expressions in `part_type(...)`");

            const auto & part_function_args = part_function_args_expr->children;
            if (part_function_args.size() != 2)
                throw_bad_arguement(1, "expected 2 arguments (`files` and `range`) in `part_type(...)`");

            const auto * files_function = part_function_args[0]->as<ASTFunction>();
            if (!files_function || files_function->name != "files")
                throw_bad_arguement(1, "expected `files(...)` in `part_type(...)`");

            const auto * files_function_args_expr = assert_cast<const ASTExpressionList *>(files_function->arguments.get());
            if (!files_function_args_expr)
                throw_bad_arguement(1, "expected list of expressions in `files(...)`");

            const auto & files_function_args = files_function_args_expr->children;
            if (files_function_args.empty())
                throw_bad_arguement(1, "expected non-zero number of arguemtns in `files(...)`");

            for (const auto & file : files_function_args)
                part.files.push_back(checkAndGetLiteralArgument<String>(file, "file_path"));

            const auto * range_function = part_function_args[1]->as<ASTFunction>();
            if (!range_function || range_function->name != "range")
                throw_bad_arguement(1, "expected `range(...)` in `part_type(...)`");

            const auto * range_function_args_expr = assert_cast<const ASTExpressionList *>(range_function->arguments.get());
            if (!range_function_args_expr)
                throw_bad_arguement(1, "expected list of expressions in `range(...)`");

            const auto & range_function_args = range_function_args_expr->children;
            if (range_function_args.size() != 2)
                throw_bad_arguement(1, "expected 2 numeric arguments in `range(x, y)`");

            part.range = {
                checkAndGetLiteralArgument<UInt64>(range_function_args[0], "range(x, _)"),
                checkAndGetLiteralArgument<UInt64>(range_function_args[1], "range(_, y)")
            };

            parts.push_back(std::move(part));
        }
    }
}

void registerTableFunctionMergeTreeParts(TableFunctionFactory & factory)
{
    factory.registerFunction<TableFunctionMergeTreeParts>();
}

}
