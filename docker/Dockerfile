FROM ubuntu
USER root
ARG DEBIAN_FRONTEND=noninteractive

# install dev tools
RUN apt-get update
RUN apt-get install -y curl tar sudo rsync net-tools wget
RUN apt install -y openjdk-8-jdk openjdk-8-jre

# # java
ENV JAVA_HOME /usr/lib/jvm/java-1.8.0-openjdk-amd64/jre
ENV PATH $PATH:$JAVA_HOME/bin


ADD ./producer.jar /usr/local/producer.jar
ADD ./dataset/data /usr/local/data


CMD cd /usr/local && java -jar producer.jar
