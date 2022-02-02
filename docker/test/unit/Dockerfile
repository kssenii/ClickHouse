# rebuild in #33610
# docker build -t clickhouse/unit-test .
ARG FROM_TAG=latest
FROM clickhouse/stateless-test:$FROM_TAG

RUN apt-get install gdb

# Download Minio-related binaries
RUN arch=${TARGETARCH:-amd64} \
    && wget "https://dl.min.io/server/minio/release/linux-${arch}/minio" \
    && chmod +x ./minio \
    && wget "https://dl.min.io/client/mc/release/linux-${arch}/mc" \
    && chmod +x ./mc

ENV MINIO_ROOT_USER="clickhouse"
ENV MINIO_ROOT_PASSWORD="clickhouse"

COPY ../stateless/setup_minio.sh /
COPY run.sh /
COPY process_unit_tests_result.py /
CMD ["/bin/bash", "/run.sh"]
