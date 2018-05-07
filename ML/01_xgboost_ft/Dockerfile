FROM continuumio/anaconda3:latest

RUN apt-get update -y -qq && apt-get install -y -qq wget bzip2 ca-certificates \
    libglib2.0-0 libxext6 libsm6 libxrender1 apt-utils \
    mercurial subversion python-setuptools make g++ reptyr tmux

WORKDIR /app
ENV PATH /opt/conda/bin:$PATH

COPY requirements.txt /app/requirements.txt

RUN pip install --upgrade pip
RUN pip install -r requirements.txt