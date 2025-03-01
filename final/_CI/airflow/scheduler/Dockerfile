FROM apache/airflow:2.10.4-python3.12

USER root

RUN apt update && \
    apt-get install -y openjdk-17-jdk && \
    apt-get install -y ant && \
    apt-get install -y pkg-config && \
    apt-get install -y libmariadb-dev && \
    apt-get install -y build-essential

ENV JAVA_HOME /usr/lib/jvm/java-17-openjdk-amd64/
RUN export JAVA_HOME

RUN mkdir -p /opt/airflow/spark/jars

RUN curl -o /opt/airflow/spark/jars/postgresql-42.2.18.jar https://jdbc.postgresql.org/download/postgresql-42.2.18.jar && \
    curl -o /opt/airflow/spark/jars/mysql-connector-java-8.3.0.jar https://repo1.maven.org/maven2/com/mysql/mysql-connector-j/8.3.0/mysql-connector-j-8.3.0.jar
    curl -o  /opt/airflow/spark/jars/mongo-spark-connector_2.12-10.1.1.jar https://repo1.maven.org/maven2/org/mongodb/spark/mongo-spark-connector_2.12/10.1.1/mongo-spark-connector_2.12-10.1.1.jar

COPY ./_CI/airflow/scheduler/requirements.txt requirements.txt
COPY .env .env

USER airflow

RUN pip install -r requirements.txt
ENV PYTHONPATH="/opt/airflow/scripts" 


CMD ["scheduler"]