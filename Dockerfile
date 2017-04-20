FROM gradle:3.5-jdk8

USER root
RUN mkdir -p /home/gradle/src
WORKDIR /home/gradle/src

COPY . /home/gradle/src
RUN gradle build

ENV KAFKA_ADDRESS "localhost:9092"

RUN tar -xvf build/distributions/testing.tar

WORKDIR /home/gradle/src/testing/lib
CMD java -cp "*" org.z.entities.engine.Main
