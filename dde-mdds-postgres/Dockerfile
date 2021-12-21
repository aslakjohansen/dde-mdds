FROM postgres:9.3
ENV POSTGRES_USER docker
ENV POSTGRES_PASSWORD docker
ENV POSTGRES_DB mdds
ADD create_db.sql /docker-entrypoint-initdb.d/

