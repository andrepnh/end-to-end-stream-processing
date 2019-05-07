from gradle:5.2.1-jdk11-slim as builder

COPY --chown=gradle:gradle build.gradle.kts settings.gradle.kts /home/gradle/app/
COPY --chown=gradle:gradle ./src /home/gradle/app/src
WORKDIR /home/gradle/app/
RUN gradle build

FROM gcr.io/distroless/java:11
COPY --from=builder /home/gradle/app/build/libs/kafka-streams-app-1.0-SNAPSHOT-all.jar /app/kafka-streams-app-1.0-SNAPSHOT.jar
WORKDIR /app
CMD ["kafka-streams-app-1.0-SNAPSHOT.jar"]