# Forked from https://github.com/flyway/flyway-docker/blob/master/Dockerfile
# because we need to add in sql files

FROM adoptopenjdk:11-jre-hotspot

# Add the flyway user and step in the directory
RUN adduser --system --home /flyway --disabled-password --group flyway
WORKDIR /flyway

# Change to the flyway user
USER flyway

ENV FLYWAY_VERSION 7.7.3

RUN curl -L https://repo1.maven.org/maven2/org/flywaydb/flyway-commandline/${FLYWAY_VERSION}/flyway-commandline-${FLYWAY_VERSION}.tar.gz -o flyway-commandline-${FLYWAY_VERSION}.tar.gz \
  && tar -xzf flyway-commandline-${FLYWAY_VERSION}.tar.gz --strip-components=1 \
  && rm flyway-commandline-${FLYWAY_VERSION}.tar.gz
  
ENV PATH="/flyway:${PATH}"

# Custom: Add in SQL
ARG SQL_FOLDER
ADD ${SQL_FOLDER} /flyway/sql/
# End Custom

ENTRYPOINT ["flyway"]
CMD ["-?"]