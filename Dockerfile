FROM gradle:jdk8-alpine

RUN echo http://dl-cdn.alpinelinux.org/alpine/edge/testing >> /etc/apk/repositories
RUN apk update
RUN apk add --update \
    libc6-compat

RUN mkdir -p /home/gradle/src
WORKDIR /home/gradle/src

COPY . /home/gradle/src
RUN gradle build

ENV KAFKA_ADDRESS "localhost:9092"

RUN tar -xvf build/distributions/testing.tar

WORKDIR /home/gradle/src/testing/lib
CMD java -cp "*" org.z.entities.engine.Main
