FROM debian:10

RUN apt-get update && apt-get install -y libssl-dev tini

COPY target/release/idgend /usr/local/bin/idgend

