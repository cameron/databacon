FROM debian:jessie

RUN apt-get -y update && apt-get -y upgrade
RUN apt-get install -y gcc git procps curl libpq-dev libffi-dev unzip make

RUN curl -O https://www.python.org/ftp/python/2.7.7/Python-2.7.7.tgz
RUN tar xvvf Python-2.7.7.tgz
RUN cd Python-2.7.7 && ./configure && make && make install

RUN curl -O https://bootstrap.pypa.io/get-pip.py
RUN python get-pip.py --prefix=/usr/local

RUN curl -O https://bitbucket.org/eradman/entr/get/1dc74ab543e2.zip
RUN unzip 1dc74ab543e2.zip && cd eradman-entr-1dc74ab543e2 && ./configure && make && make install

RUN git clone https://github.com/teepark/datahog
RUN cd datahog; git checkout 5d51fc8d9dc1; python setup.py install

ADD requirements.txt /requirements.txt

RUN pip install -r /requirements.txt

ADD . /src

CMD ["/bin/bash -c 'sleep 9999'"]



