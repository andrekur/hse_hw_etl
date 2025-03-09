FROM python:3.13.1-alpine

RUN pip install --upgrade pip

COPY ./data_gen/ ./data_gen

WORKDIR /data_gen

COPY ./_CI/generator/requirements.txt ./requirements.txt
COPY ./.env ./.env

RUN pip install -r ./requirements.txt

CMD ["python", "main.py"]
