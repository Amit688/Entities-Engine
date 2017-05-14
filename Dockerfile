FROM gradle:jdk8-alpine

RUN mkdir -p /home/gradle/src
WORKDIR /home/gradle/src

COPY . /home/gradle/src
RUN gradle build
RUN tar -xvf build/distributions/testing.tar

ENV KAFKA_ADDRESS "localhost:9092"
ENV JMX_PORT "9010"
ENV KAMON_ENABLED yes

WORKDIR /home/gradle/src/testing/lib
CMD java -Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.port=$JMX_PORT -Dcom.sun.management.jmxremote.local.only=false -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false -cp "*" org.z.entities.engine.Main;
