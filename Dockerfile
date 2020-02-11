#
# Paleobiology database: archiver API image
# 
# The image 'paleobiodb_archiver_preload' can be built using the file 'Dockerfile-preload'.
# See that file for more information.

FROM paleobiodb_archiver_preload

COPY pbdb-archiver /usr/src/app

EXPOSE 6002

CMD uwsgi /usr/src/app/archiver-wsgi.ini

# CMD sleep 3600

LABEL maintainer="mmcclenn@geology.wisc.edu"
LABEL version="1.0"
LABEL description="Paleobiology Database Archiver API"

LABEL buildcheck="uwsgi /usr/src/app/archiver-wsgi.ini"

