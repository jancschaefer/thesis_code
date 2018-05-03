FROM continuumio/anaconda3:latest

RUN apt-get update -y -qq && apt-get install -y -qq wget bzip2 ca-certificates \
    libglib2.0-0 libxext6 libsm6 libxrender1 apt-utils \
    mercurial subversion python-setuptools make g++

WORKDIR /app

#RUN git clone --recursive https://github.com/dmlc/xgboost && \\
#        cd xgboost/xgboost && make -j4

ENV PATH /opt/conda/bin:$PATH
#RUN conda install -y -c https://conda.anaconda.org/anaconda setuptools


COPY requirements.txt /app/requirements.txt

RUN pip install --upgrade pip
RUN pip install -r requirements.txt

RUN apt-get install -y -qq reptyr tmux

#RUN "/opt/conda/bin/conda install jupyter -y --quiet && mkdir /opt/notebooks"

