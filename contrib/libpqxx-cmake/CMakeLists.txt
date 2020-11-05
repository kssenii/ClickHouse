set (LIBRARY_DIR ${ClickHouse_SOURCE_DIR}/contrib/libpqxx)

set (SRCS
        ${LIBRARY_DIR}/src/array.cxx
        ${LIBRARY_DIR}/src/binarystring.cxx
        ${LIBRARY_DIR}/src/connection.cxx
        ${LIBRARY_DIR}/src/cursor.cxx
        ${LIBRARY_DIR}/src/encodings.cxx
        ${LIBRARY_DIR}/src/errorhandler.cxx
        ${LIBRARY_DIR}/src/except.cxx
        ${LIBRARY_DIR}/src/field.cxx
        ${LIBRARY_DIR}/src/largeobject.cxx
        ${LIBRARY_DIR}/src/notification.cxx
        ${LIBRARY_DIR}/src/pipeline.cxx
        ${LIBRARY_DIR}/src/result.cxx
        ${LIBRARY_DIR}/src/robusttransaction.cxx
        ${LIBRARY_DIR}/src/sql_cursor.cxx
        ${LIBRARY_DIR}/src/strconv.cxx
        ${LIBRARY_DIR}/src/stream_from.cxx
        ${LIBRARY_DIR}/src/stream_to.cxx
        ${LIBRARY_DIR}/src/subtransaction.cxx
        ${LIBRARY_DIR}/src/transaction.cxx
        ${LIBRARY_DIR}/src/transaction_base.cxx
        ${LIBRARY_DIR}/src/row.cxx
        ${LIBRARY_DIR}/src/util.cxx
        ${LIBRARY_DIR}/src/version.cxx
        )

#add_definitions(-DUSE_STD_NAMESPACE=1)
#add_definitions(-DCMAKE_FIND_FRAMEWORK=LAST)
#set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++17 -stdlib=libc++")

add_library(libpqxx ${SRCS})
target_link_libraries(libpqxx PRIVATE ${LIBPQ_LIBRARY})

#target_compile_options (libpqxx
#        PRIVATE
#        -Wno-old-style-cast
#        -Wno-inconsistent-missing-destructor-override
#        -Wno-deprecated
#        -Wno-unused-parameter
#        -Wno-shadow
#        -Wno-tautological-type-limit-compare
#        -Wno-extra-semi
#        # NOTE: disable all warnings at last because the warning:
#        #       "conversion function converting 'XXX' to itself will never be used"
#        #       doesn't have it's own diagnostic flag yet.
#        -w
#        )
##
##if (OS_DARWIN)
##    target_include_directories (libpqxx SYSTEM PRIVATE ${LIBRARY_DIR}/../libpqxx-cmake/darwin)
##endif()
#
target_include_directories (libpqxx SYSTEM BEFORE PRIVATE ${LIBRARY_DIR}/include)
target_include_directories (libpqxx PRIVATE ${LIBRARY_DIR}/include/pqxx)
target_include_directories (libpqxx PRIVATE ${LIBRARY_DIR}/include/pqxx)

