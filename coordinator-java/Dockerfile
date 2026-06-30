FROM maven:3.9-eclipse-temurin-21 AS build

WORKDIR /app
COPY pom.xml ./
RUN mvn -q -DskipTests dependency:go-offline
COPY src ./src
RUN mvn -q -DskipTests package

FROM eclipse-temurin:21-jre

WORKDIR /app
COPY --from=build /app/target/coordinator-0.1.0.jar /app/coordinator.jar
COPY --from=build /app/target/dependency /app/dependency

EXPOSE 8088
CMD [ \
    "java", \
    "--add-opens=java.base/java.nio=ALL-UNNAMED", \
    "-Dorg.slf4j.simpleLogger.defaultLogLevel=warn", \
    "-cp", \
    "/app/coordinator.jar:/app/dependency/*", \
    "com.arrowflight.coordinator.CoordinatorApplication" \
]
