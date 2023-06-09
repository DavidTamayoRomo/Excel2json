## Dockerfile-provider

# Maven 
#FROM maven:3.8.1-openjdk-11-slim AS builder
FROM maven:3.8.4-openjdk-17-slim AS builder

WORKDIR /app

COPY pom.xml .

RUN mvn -e -B dependency:resolve

COPY src ./src

RUN mvn clean -e -B package


# RTSDK Java
FROM openjdk:17

WORKDIR /app

ENV TZ=America/Guayaquil
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

COPY --from=builder /app/target/mdmq_cmi_masivo-1.0.jar .

EXPOSE 8071

CMD ["java", "-jar", "./mdmq_cmi_masivo-1.0.jar"]