FROM apache/airflow:2.10.4-python3.12

COPY ./_CI/airflow/init/init.sh ./init_airflow.sh
COPY ./.env ./.env

USER root
RUN mkdir -p /usr/local/airflow/db
RUN chown 50000 /usr/local/airflow/db

RUN mkdir -p /usr/local/airflow/dags
RUN chown 50000 /usr/local/airflow/dags

RUN mkdir -p /usr/local/airflow/scripts
RUN chown 50000 /usr/local/airflow/scripts

RUN chown 50000 /usr/bin
USER airflow
CMD ["bash", "./init_airflow.sh"]