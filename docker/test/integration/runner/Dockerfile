# docker build -t clickhouse/integration-tests-runner .
FROM ubuntu:20.04

# ARG for quick switch to a given ubuntu mirror
ARG apt_archive="http://archive.ubuntu.com"

RUN sed -i "s|http://archive.ubuntu.com|$apt_archive|g" /etc/apt/sources.list

RUN apt-get update \
    && env DEBIAN_FRONTEND=noninteractive apt-get install --yes \
    adduser \
    ca-certificates \
    bash \
    btrfs-progs \
    e2fsprogs \
    iptables \
    xfsprogs \
    tar \
    pigz \
    wget \
    git \
    nmap \
    iproute2 \
    cgroupfs-mount \
    python3-pip \
    tzdata \
    libicu-dev \
    bsdutils \
    curl \
    python3-pika \
    liblua5.1-dev \
    luajit \
    libssl-dev \
    libcurl4-openssl-dev \
    gdb \
    default-jdk \
    software-properties-common \
    libkrb5-dev \
    krb5-user \
    g++ \
    && rm -rf \
        /var/lib/apt/lists/* \
        /var/cache/debconf \
        /tmp/* \
    && apt-get clean

ENV TZ=Etc/UTC
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

ENV DOCKER_CHANNEL stable
RUN curl -fsSL https://download.docker.com/linux/ubuntu/gpg | apt-key add - \
    && add-apt-repository "deb https://download.docker.com/linux/ubuntu $(lsb_release -c -s) ${DOCKER_CHANNEL}" \
    && apt-get update \
    && env DEBIAN_FRONTEND=noninteractive apt-get install --yes \
        docker-ce \
    && rm -rf \
        /var/lib/apt/lists/* \
        /var/cache/debconf \
        /tmp/* \
    && apt-get clean

RUN dockerd --version; docker --version

RUN python3 -m pip install --no-cache-dir \
    PyMySQL \
    aerospike==4.0.0 \
    avro==1.10.2 \
    asyncio \
    cassandra-driver \
    confluent-kafka==1.5.0 \
    dict2xml \
    dicttoxml \
    docker \
    docker-compose==1.29.2 \
    grpcio \
    grpcio-tools \
    kafka-python \
    kazoo \
    lz4 \
    minio \
    nats-py \
    protobuf \
    psycopg2-binary==2.8.6 \
    pymongo==3.11.0 \
    pytest \
    pytest-order==1.0.0 \
    pytest-timeout \
    pytest-random \
    pytest-xdist \
    pytest-repeat \
    pytz \
    redis \
    tzlocal==2.1 \
    urllib3 \
    requests-kerberos \
    pyspark==3.3.2 \
    delta-spark==2.2.0 \
    pyhdfs \
    azure-storage-blob \
    meilisearch==0.18.3

COPY modprobe.sh /usr/local/bin/modprobe
COPY dockerd-entrypoint.sh /usr/local/bin/
COPY compose/ /compose/
COPY misc/ /misc/

ENV SPARK_VERSION=3.3.2
ENV SPARK_MAJOR_VERSION=3.3
ENV ICEBERG_VERSION=1.2.1

# Download Java AWS SDK
ENV AWSSDK_VERSION=2.20.18
#RUN curl https://repo1.maven.org/maven2/software/amazon/awssdk/bundle/${AWSSDK_VERSION}/bundle-${AWSSDK_VERSION}.jar -Lo /opt/spark/jars/bundle-${AWSSDK_VERSION}.jar

RUN curl -fsSL -O https://dlcdn.apache.org/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz \
    && tar xzvf spark-${SPARK_VERSION}-bin-hadoop3.tgz -C / \
    && rm spark-${SPARK_VERSION}-bin-hadoop3.tgz

# download spark and packages
# if you change packages, don't forget to update them in tests/integration/helpers/cluster.py
RUN echo ":quit" | /spark-${SPARK_VERSION}-bin-hadoop3/bin/spark-shell --packages "\
org.apache.hudi:hudi-spark3.3-bundle_2.12:0.13.0,\
io.delta:delta-core_2.12:2.2.0,\
org.apache.iceberg:iceberg-spark-runtime-${SPARK_MAJOR_VERSION}_2.12:${ICEBERG_VERSION},\
software.amazon.awssdk:bundle:${AWSSDK_VERSION},\
com.amazonaws:aws-java-sdk:1.12.4,\
#apache.hadoop:hadoop-aws:3.3.5,\
software.amazon.awssdk:url-connection-client:${AWSSDK_VERSION}" > /dev/null

ENV SPARK_HOME="/spark-3.3.2-bin-hadoop3"
#RUN curl https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-${SPARK_MAJOR_VERSION}_2.12/${ICEBERG_VERSION}/iceberg-spark-runtime-${SPARK_MAJOR_VERSION}_2.12-${ICEBERG_VERSION}.jar -Lo /opt/spark/jars/iceberg-spark-runtime-${SPARK_MAJOR_VERSION}_2.12-${ICEBERG_VERSION}.jar
#RUN curl https://repo1.maven.org/maven2/software/amazon/awssdk/url-connection-client/${AWSSDK_VERSION}/url-connection-client-${AWSSDK_VERSION}.jar -Lo /opt/spark/jars/url-connection-client-${AWSSDK_VERSION}.jar

ENV AWS_ACCESS_KEY_ID=minio
ENV AWS_SECRET_ACCESS_KEY=minio123
ENV AWS_REGION="eu-east-1"

RUN set -x \
  && addgroup --system dockremap \
    && adduser --system dockremap \
  && adduser dockremap dockremap \
  && echo 'dockremap:165536:65536' >> /etc/subuid \
    && echo 'dockremap:165536:65536' >> /etc/subgid

# Same options as in test/base/Dockerfile
# (in case you need to override them in tests)
ENV TSAN_OPTIONS='halt_on_error=1 history_size=7 memory_limit_mb=46080 second_deadlock_stack=1'
ENV UBSAN_OPTIONS='print_stacktrace=1'
ENV MSAN_OPTIONS='abort_on_error=1 poison_in_dtor=1'

EXPOSE 2375
ENTRYPOINT ["dockerd-entrypoint.sh"]
CMD ["sh", "-c", "pytest $PYTEST_OPTS"]
