set (LIBRARY_DIR ${ClickHouse_SOURCE_DIR}/contrib/nanodbc)

set (SRCS
    ${LIBRARY_DIR}/nanodbc/nanodbc.cpp
)

#set (HDRS
#    ${LIBRARY_DIR}/nanodbc/nanodbc.h
#)

add_library(nanodbc ${SRCS})

target_compile_options (nanodbc
    PRIVATE
        -Wno-old-style-cast
        -Wno-inconsistent-missing-destructor-override
        -Wno-deprecated
        -Wno-unused-parameter
        -Wno-shadow
        -Wno-tautological-type-limit-compare
        -Wno-extra-semi
        -Wno-error
        -Wno-documentation
# NOTE: disable all warnings at last because the warning:
        #       "conversion function converting 'XXX' to itself will never be used"
        #       doesn't have it's own diagnostic flag yet.
        -w
)

target_link_libraries(nanodbc PUBLIC ${IODBC_LIBRARY})
target_include_directories (nanodbc PUBLIC ${LIBRARY_DIR}/)

