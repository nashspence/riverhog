FROM debian:bookworm-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends bash ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=dxflrs/garage:v2.2.0 /garage /usr/local/bin/garage
COPY init-garage.sh /usr/local/bin/init-garage.sh

RUN chmod +x /usr/local/bin/init-garage.sh

ENTRYPOINT ["/usr/local/bin/init-garage.sh"]
