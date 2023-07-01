FROM ubuntu:20.04

# Update packages and install Java
RUN apt-get update && apt-get install -y openjdk-8-jdk

# Create project directory and go to 
mkdir -p /lab2
WORKDIR /lab2

# Download Hadoop & extract
RUN wget https://dlcdn.apache.org/hadoop/common/hadoop-3.3.1/hadoop-3.3.1.tar.gz

RUN tar -xzf hadoop-3.3.1.tar.gz

# Go to Hadoop directory
WORKDIR /lab2/hadoop-3.3.1

# Change Java-related environment variables
ENV JAVA_HOME=$(dirname $(dirname $(readlink -f $(which javac))))

RUN echo "export JAVA_HOME=${JAVA_HOME}" >> etc/hadoop/hadoop-env.sh

# Test if Hadoop is installed properly
RUN bin/hadoop

# Create bash entrypoint for ease of use
ENTRYPOINT [ "/bin/bash" ]
