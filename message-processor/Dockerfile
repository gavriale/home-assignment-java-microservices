# Build context is the repo root (see docker/docker-compose.yml) so this stage can reach the
# parent pom and the contracts module that message-processor depends on.
FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /workspace

COPY pom.xml .
COPY contracts contracts
COPY message-api message-api
COPY message-processor message-processor

RUN mvn -q -B -pl message-processor -am -DskipTests package \
    && cp message-processor/target/message-processor-*.jar application.jar \
    && java -Djarmode=tools -jar application.jar extract --layers --destination extracted

FROM eclipse-temurin:21-jre-jammy
RUN apt-get update \
    && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --system spring \
    && useradd --system --gid spring --no-create-home spring

WORKDIR /application
COPY --from=build /workspace/extracted/dependencies/ ./
COPY --from=build /workspace/extracted/spring-boot-loader/ ./
COPY --from=build /workspace/extracted/snapshot-dependencies/ ./
COPY --from=build /workspace/extracted/application/ ./

USER spring:spring
EXPOSE 8082
ENTRYPOINT ["java", "-jar", "application.jar"]
