FROM openjdk:8

WORKDIR /usr/local
ADD https://github.com/Amit688/Entities-Engine/releases/download/0.0.1/testing.tar .
RUN tar -xvf testing.tar

ENV KAFKA_ADDRESS "localhost:9092"

CMD "/usr/local/testing/bin/testing"
