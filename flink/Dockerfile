FROM flink:1.18.1-scala_2.12-java11

USER root

# Python + toolchain needed to install PyFlink (pemja builds against JAVA_HOME)
RUN apt-get update \
 && apt-get install -y --no-install-recommends \
      python3 python3-pip python3-dev build-essential curl ca-certificates \
 && ln -sf /usr/bin/python3 /usr/bin/python \
 && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/opt/java/openjdk

RUN pip3 install --no-cache-dir apache-flink==1.18.1

# Connector JARs must match the Flink minor version (1.18).
ARG MAVEN=https://repo.maven.apache.org/maven2
RUN set -eux; cd /opt/flink/lib; \
    curl -fsSLO $MAVEN/org/apache/flink/flink-sql-connector-kafka/3.1.0-1.18/flink-sql-connector-kafka-3.1.0-1.18.jar; \
    curl -fsSLO $MAVEN/org/apache/flink/flink-connector-jdbc/3.1.2-1.18/flink-connector-jdbc-3.1.2-1.18.jar; \
    curl -fsSLO $MAVEN/org/postgresql/postgresql/42.7.3/postgresql-42.7.3.jar

# Prometheus metrics reporter ships in /opt/flink/opt and must be loaded as a plugin.
RUN mkdir -p /opt/flink/plugins/metrics-prometheus \
 && cp /opt/flink/opt/flink-metrics-prometheus-1.18.1.jar /opt/flink/plugins/metrics-prometheus/

# Cluster + client configuration. Baked into the image so the job *client*
# (which builds the job graph) and the cluster see identical settings.
COPY conf/flink-conf.append.yaml /tmp/flink-conf.append.yaml
RUN sed -i -E '/^(parallelism\.default|taskmanager\.numberOfTaskSlots|jobmanager\.memory\.process\.size|taskmanager\.memory\.process\.size):/d' /opt/flink/conf/flink-conf.yaml \
 && cat /tmp/flink-conf.append.yaml >> /opt/flink/conf/flink-conf.yaml \
 && rm /tmp/flink-conf.append.yaml

# Pre-create state dirs so named volumes inherit flink:flink ownership.
RUN mkdir -p /opt/flink/checkpoints /opt/flink/savepoints \
 && chown -R flink:flink /opt/flink/checkpoints /opt/flink/savepoints

COPY --chown=flink:flink jobs/ /opt/flink/jobs/
COPY --chown=flink:flink submit-job.sh /opt/flink/bin/submit-job.sh
RUN chmod +x /opt/flink/bin/submit-job.sh

USER flink
