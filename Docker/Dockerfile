#
# Python Dockerfile
#
# https://github.com/dockerfile/python
#

# Pull base image.
FROM continuumio/miniconda

# Adding SSH Keys
ADD ./id_rsa /root/.ssh/id_rsa
ADD ./id_rsa.pub /root/.ssh/id_rsa.pub
ADD /Users/janschaefer/Dropbox/10_Thesis/02_Data/data.parquet /root/02_Data/data.parquet
ADD /Users/janschaefer/Dropbox/10_Thesis/iterate.txt /root/iterate.txt

# Install Python.
RUN \
  apt-get update -y && \
  apt-get upgrade -y && \
  pip install --upgrade pip && \
  pip install pandas numpy logging pyarrow http && \
  rm -rf /var/lib/apt/lists/* && \
  chmod 400 ~/.ssh/id_rsa && \
  ssh -T git@github.com -oStrictHostKeyChecking=no && \
  cd /root && \
  git clone git@github.com:jancschaefer/thesis_code.git && \
  mv thesis_code/ Code 
  
# Define working directory.
WORKDIR /root

# Define default command.
CMD ["bash"]