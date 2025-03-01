FROM python:3.13.1-alpine

RUN pip install --upgrade pip

COPY ./data_generators/db_data_gen/ ./db_data_gen

WORKDIR /db_data_gen

COPY ./_CI/generator/requirements.txt ./requirements.txt
COPY ./.env ./.env

RUN pip install -r ./requirements.txt

CMD ["python", "main.py"]
