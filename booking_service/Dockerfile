FROM python:3.9.5-slim-buster


# set environment variables
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

# copy project
COPY ./requirements.txt ./

# install dependencies
RUN apt-get update
RUN apt-get install poppler-utils -y
RUN apt-get install g++ -y

RUN pip install --upgrade pip
RUN pip install -r requirements.txt