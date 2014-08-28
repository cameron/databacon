FROM debian:wheezy

ADD pgapt.list /etc/apt/sources.list.d/pgapt.list
ADD pgapt.asc /pgapt.asc

RUN apt-key add /pgapt.asc
RUN apt-get -y update && apt-get -y upgrade && apt-get install -y postgresql-9.3 postgresql-contrib-9.3

ADD start /start
ADD shard.up.sql /shard.up.sql

EXPOSE 5432

CMD ["/bin/sh", "/start"]
