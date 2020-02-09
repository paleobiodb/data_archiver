#
# Paleobiology database: archiver API

FROM python:3-alpine AS paleobiodb_archiver_preload

RUN mkdir -p /usr/src/app
WORKDIR /usr/src/app

COPY pbdb-archiver/requirements.txt /usr/src/app/

RUN apk add python3-dev build-base linux-headers pcre-dev mariadb-dev mariadb-client
RUN pip install uwsgi
RUN pip install --no-cache-dir -r requirements.txt

FROM paleobiodb_archiver_preload

COPY pbdb-archiver /usr/src/app

EXPOSE 6002

CMD uwsgi /usr/src/app/archiver-wsgi.ini

# CMD sleep 3600

LABEL maintainer="mmcclenn@geology.wisc.edu"
LABEL version="1.0"
LABEL description="Paleobiology Database Archiver API"

LABEL buildcheck="uwsgi /usr/src/app/archiver-wsgi.ini"

