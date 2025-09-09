FROM ubuntu:24.04

RUN apt-get update \
    && apt-get -y install \
        make \
        automake \
        libtool \
        pkg-config \
        libaio-dev \
        git \
        libmysqlclient-dev \
        libssl-dev \
        libpq-dev \
        mysql-client

RUN git clone https://github.com/Tusamarco/sysbench.git sysbench

WORKDIR sysbench
RUN ./autogen.sh \
    && ./configure --with-mysql --with-pgsql \
    && make -j \
    && make install

ENTRYPOINT sysbench
