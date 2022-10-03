FROM python:3.9-buster

WORKDIR /home/python/app

RUN apt-get update -y && apt-get upgrade -y

RUN pip3 install mysql-connector &&\
    pip3 install SQLAlchemy &&\
    pip3 install python-dotenv &&\
    pip3 install pymysql &&\
    pip3 install psycopg2

RUN pip3 install cassandra-driver

CMD [ "tail", "-f", "/dev/null" ]