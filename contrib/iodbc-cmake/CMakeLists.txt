set(IODBC_SOURCE_DIR ${ClickHouse_SOURCE_DIR}/contrib/iODBC)

set(SRCS
    ${IODBC_SOURCE_DIR}/iodbc/catalog.c
    ${IODBC_SOURCE_DIR}/iodbc/connect.c
    ${IODBC_SOURCE_DIR}/iodbc/dlproc.c
    ${IODBC_SOURCE_DIR}/iodbc/execute.c
    ${IODBC_SOURCE_DIR}/iodbc/fetch.c
    ${IODBC_SOURCE_DIR}/iodbc/hdbc.c
    ${IODBC_SOURCE_DIR}/iodbc/henv.c
    ${IODBC_SOURCE_DIR}/iodbc/herr.c
    ${IODBC_SOURCE_DIR}/iodbc/hstmt.c
    ${IODBC_SOURCE_DIR}/iodbc/info.c
)

set(HDRS
    ${IODBC_SOURCE_DIR}/iodbc/dlproc.h
    ${IODBC_SOURCE_DIR}/iodbc/hdbc.h
    ${IODBC_SOURCE_DIR}/iodbc/hdesc.h
    ${IODBC_SOURCE_DIR}/iodbc/henv.h
    ${IODBC_SOURCE_DIR}/iodbc/herr.h
    ${IODBC_SOURCE_DIR}/iodbc/hstmt.h
    ${IODBC_SOURCE_DIR}/iodbc/itrace.h
)

add_library(iodbc ${SRCS} ${HDRS})

target_include_directories (iodbc PUBLIC ${IODBC_SOURCE_DIR})
target_include_directories (iodbc PUBLIC ${IODBC_SOURCE_DIR}/include)

target_include_directories (iodbc PRIVATE ${IODBC_SOURCE_DIR}/iodbc)
target_include_directories (iodbc PRIVATE ${IODBC_SOURCE_DIR}/iodbcinst)
