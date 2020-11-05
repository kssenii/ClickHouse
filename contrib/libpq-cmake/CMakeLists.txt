set(LIBPQ_SOURCE_DIR ${ClickHouse_SOURCE_DIR}/contrib/libpq)

set(SRCS
        ${LIBPQ_SOURCE_DIR}/fe-auth.c
        ${LIBPQ_SOURCE_DIR}/fe-auth-scram.c
        ${LIBPQ_SOURCE_DIR}/fe-connect.c
        ${LIBPQ_SOURCE_DIR}/fe-exec.c
        ${LIBPQ_SOURCE_DIR}/fe-gssapi-common.c
        ${LIBPQ_SOURCE_DIR}/fe-lobj.c
        ${LIBPQ_SOURCE_DIR}/fe-misc.c
        ${LIBPQ_SOURCE_DIR}/fe-print.c
        ${LIBPQ_SOURCE_DIR}/fe-protocol2.c
        ${LIBPQ_SOURCE_DIR}/fe-protocol3.c
        ${LIBPQ_SOURCE_DIR}/fe-secure.c
        ${LIBPQ_SOURCE_DIR}/fe-secure-common.c
        ${LIBPQ_SOURCE_DIR}/fe-secure-gssapi.c
        ${LIBPQ_SOURCE_DIR}/fe-secure-openssl.c
        ${LIBPQ_SOURCE_DIR}/legacy-pqsignal.c
        ${LIBPQ_SOURCE_DIR}/libpq-events.c
        ${LIBPQ_SOURCE_DIR}/pqexpbuffer.c
        )

add_library(libpq ${SRCS})

if (OS_LINUX)
    target_include_directories (libpq SYSTEM PRIVATE ${LIBPQ_SOURCE_DIR}/include/linux)
elseif (OS_DARWIN)
    target_include_directories (libpq SYSTEM PRIVATE ${LIBPQ_SOURCE_DIR}/include/darwin)
endif()

target_include_directories (libpq PUBLIC ${LIBPQ_SOURCE_DIR})
target_include_directories (libpq PUBLIC ${LIBPQ_SOURCE_DIR}/include)

target_include_directories (libpq PRIVATE ${LIBPQ_SOURCE_DIR}/include/common)
target_include_directories (libpq PRIVATE ${LIBPQ_SOURCE_DIR}/include/libpq)

#configure_file(${LIBPQ_SOURCE_DIR}/include/pg_config.h.in ${LIBPQ_SOURCE_DIR}/include/pg_config.h)
#configure_file(${LIBPQ_SOURCE_DIR}/include/pg_config_ext.h.in ${LIBPQ_SOURCE_DIR}/include/pg_config_ext.h)

target_link_libraries (libpq PRIVATE ssl)
