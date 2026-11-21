# Bootstrap environment for the repository's unchanged make linux-qualification.
# Project tools and Python dependencies still come from mise.lock and uv.lock.
FROM docker:29.3.1-cli@sha256:18f5ab0fab739ea822819b342357947dfba235cdef438cce345ebc0c143c5b34 AS docker-cli
FROM docker/buildx-bin:0.36.0@sha256:a7622548b38cb1826781a466e3f227476e85ab50b8009170b63d4f17bd4b3bc6 AS buildx
FROM docker/compose-bin:v5.1.1@sha256:dd76c8f40b1e3b43277c978d5cb7bd3a98e4739e97837e5e3c9e1379e85399a8 AS compose
FROM jdxcode/mise:2026.11.2@sha256:39cd8aad519a3dbc860eceb64dfd8cb5efe4cdbb8ad741a6867b544b2787b79c AS mise-bootstrap
FROM ubuntu:24.04@sha256:fe81560990fae86dc4743202b2cb4d3df98d0bcf949173cecef15ce1127ef852

RUN apt-get update \
    && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
        build-essential \
        ca-certificates \
        curl \
        dbus-user-session \
        file \
        git \
        jq \
        libffi-dev \
        libmagic1t64 \
        libssl-dev \
        make \
        perl \
        pkg-config \
        python3 \
        sudo \
        systemd \
        unzip \
        util-linux \
        xz-utils \
        zip \
    && rm -rf /var/lib/apt/lists/* \
    && test "$(id -u ubuntu)" = 1000 \
    && install -d -o 1000 -g 1000 /workspace

COPY --from=mise-bootstrap /usr/local/bin/mise /usr/local/bin/mise
COPY --from=docker-cli /usr/local/bin/docker /usr/local/bin/docker
COPY --from=buildx /buildx /usr/local/lib/docker/cli-plugins/docker-buildx
COPY --from=compose /docker-compose /usr/local/lib/docker/cli-plugins/docker-compose
COPY --chmod=0555 check-image.sh /usr/local/libexec/riverhog-qualification-check

# systemd is the container's init, not the identity running qualification.
# The runtime provider starts the unprivileged user manager and delegates cgroups.
ENV container=docker
WORKDIR /workspace
STOPSIGNAL SIGRTMIN+3
ENTRYPOINT ["/usr/lib/systemd/systemd"]
CMD ["--unit=basic.target", "--show-status=no"]
