ARG IMAGE_VARIANT=buster
ARG OPENJDK_VERSION=8
ARG PYTHON_VERSION=3.9.8

FROM python:${PYTHON_VERSION}-${IMAGE_VARIANT} AS py3
FROM openjdk:${OPENJDK_VERSION}-${IMAGE_VARIANT}

COPY --from=py3 / /

RUN apt-get update -y && apt-get upgrade -y
RUN apt-get install vim -y && apt-get install nano -y && apt-get install wget

ARG PYSPARK_VERSION=3.2.0
RUN pip --no-cache-dir install pyspark==${PYSPARK_VERSION}

# RUN apt-get remove scala-library scala
# RUN wget https://downloads.lightbend.com/scala/2.12.3/scala-2.12.3.deb
# RUN dpkg -i scala-2.12.3.deb
# RUN apt-get update
# RUN apt-get install scala


RUN pip3 install mysql-connector &&\
    pip3 install SQLAlchemy &&\
    pip3 install python-dotenv &&\
    pip3 install pymysql &&\
    pip3 install psycopg2 &&\
    pip3 install pandas &&\
    pip3 install cassandra-driver &&\
    pip3 install cryptography &&\
    pip3 install openpyxl &&\
    pip3 install xlsxwriter
    
CMD [ "tail", "-f", "/dev/null" ]