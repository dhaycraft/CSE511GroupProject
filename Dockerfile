FROM bigtruedata/sbt:latest

ARG DEBIAN_FRONTEND=noninteractive
RUN apt-get update
RUN apt-get install wget zip unzip
RUN wget https://downloads.apache.org/spark/spark-3.1.2/spark-3.1.2-bin-hadoop3.2.tgz
RUN tar xvf spark-*
RUN mv spark-3.1.2-bin-hadoop3.2 /opt/spark
ENV SPARK_HOME=/opt/spark
RUN echo "export PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin" >> ~/.profile
RUN echo "export PYSPARK_PYTHON=/usr/bin/python3" >> ~/.profile
RUN bash
RUN git clone https://github.com/dhaycraft/CSE511GroupProject.git
RUN source ~/.profile
ENTRYPOINT /root/CSE511GroupProject/CSE511-Project-Hotspot-Analysis/run.sh; /bin/bash
