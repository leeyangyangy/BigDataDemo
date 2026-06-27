# syntax=docker/dockerfile:1
# ============ build stage ============
FROM maven:3.9-eclipse-temurin-11 AS build
WORKDIR /build

# cache layer: resolve dependencies first (only re-runs when pom changes)
COPY pom.xml .
RUN mvn -B dependency:go-offline

# compile & package (skip tests for image build)
COPY src ./src
RUN mvn -B clean package -DskipTests \
 && mv target/cockpit-backend-*.jar /app.jar

# ============ runtime stage ============
FROM eclipse-temurin:11-jre
WORKDIR /app

ENV TZ=Asia/Shanghai \
    JAVA_OPTS="-Xms256m -Xmx1024m -Duser.timezone=Asia/Shanghai"

COPY --from=build /app.jar /app/app.jar

EXPOSE 8080

ENTRYPOINT ["sh", "-c", "java $JAVA_OPTS -jar /app/app.jar"]
