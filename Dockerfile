FROM eclipse-temurin:21-jdk AS build
WORKDIR /app
COPY pom.xml .
COPY streamhub-protocol streamhub-protocol
COPY streamhub-storage streamhub-storage
COPY streamhub-coordinator streamhub-coordinator
COPY streamhub-broker streamhub-broker
RUN apt-get update && apt-get install -y maven && \
    mvn -q -pl streamhub-protocol,streamhub-storage,streamhub-coordinator,streamhub-broker -am package -DskipTests

FROM eclipse-temurin:21-jre
WORKDIR /app
COPY --from=build /app/streamhub-broker/target/streamhub-broker-1.0.0-SNAPSHOT.jar broker.jar
RUN mkdir -p /data
ENV STREAMHUB_DATA=/data STREAMHUB_PORT=9092 STREAMHUB_HEALTH_PORT=8080
EXPOSE 9092 8080
ENTRYPOINT ["java", "-jar", "broker.jar"]
