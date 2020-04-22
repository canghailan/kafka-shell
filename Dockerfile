FROM maven:3-jdk-8 AS build-stage

WORKDIR /workspace/

COPY pom.xml .
RUN mvn verify clean --fail-never

COPY . .
RUN mvn package -Dmaven.test.skip=true

FROM registry.cn-hangzhou.aliyuncs.com/yitong/java:openjdk-8

ENV JAVA_OPTS -Xmx256m

WORKDIR /app/

COPY --from=build-stage /workspace/target/*jar-with-dependencies.jar /app/app.jar

CMD java -jar $JAVA_OPTS /app/app.jar
