FROM debian:jessie

RUN apt-get -y update && apt-get -y upgrade
RUN apt-get install -y gcc git procps curl libpq-dev libffi-dev unzip make

RUN curl -O https://www.python.org/ftp/python/3.6.1/Python-3.6.1.tgz
RUN tar xvvf Python-3.6.1.tgz
RUN cd Python-3.6.1 && ./configure && make && make install

# TODO remove?
#RUN curl -O https://bootstrap.pypa.io/get-pip.py
#RUN python get-pip.py --prefix=/usr/local

# TODO uncomment later
#RUN git clone https://github.com/cameron/datahog
#RUN cd datahog; python3 setup.py install

ADD requirements.txt /requirements.txt

RUN pip3 install -r /requirements.txt

ADD . /src

CMD ["/bin/bash -c '/bin/sleep 9999999999999999999'"]



