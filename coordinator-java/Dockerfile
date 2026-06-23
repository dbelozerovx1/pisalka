FROM eclipse-temurin:21-jdk AS build

WORKDIR /app
COPY src ./src
RUN find src/main/java -name '*.java' -print > sources.txt \
    && javac --release 21 -d out @sources.txt \
    && jar --create --file coordinator.jar --main-class com.arrowflight.coordinator.CoordinatorApplication -C out .

FROM eclipse-temurin:21-jre

WORKDIR /app
COPY --from=build /app/coordinator.jar /app/coordinator.jar

EXPOSE 8088
CMD ["java", "-jar", "/app/coordinator.jar"]
