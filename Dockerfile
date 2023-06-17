FROM openjdk:19
MAINTAINER KulikovPavel
COPY build/libs/JavaServiceEnrichment-1.0-SNAPSHOT.jar Enrichment.jar
ENTRYPOINT ["java","-jar","/Enrichment.jar"]