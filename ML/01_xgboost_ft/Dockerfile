FROM continuumio/anaconda3:latest

#ENV http_proxy http://proxy.henkelgroup.net:80
#ENV https_proxy http://proxy.henkelgroup.net:80

RUN apt-get update -y --force-yes -qq && apt-get install -y --force-yes -qq wget bzip2 ca-certificates \
    libglib2.0-0 libxext6 libsm6 libxrender1 apt-utils \
    mercurial subversion python-setuptools make g++

WORKDIR /app
ENV PATH /opt/conda/bin:$PATH

COPY requirements.txt /app/requirements.txt

RUN pip install --upgrade pip
RUN pip install -r requirements.txt