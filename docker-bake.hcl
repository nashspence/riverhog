group "default" {
  targets = [
    "riverhog",
    "a-riverhog-ftp-spool",
    "a-riverhog-aws-store",
    "a-riverhog-b2-store",
    "a-riverhog-filesystem-store",
    "stove0",
    "a-stove0-exiftool-observer",
    "a-stove0-ffprobe-sampling-observer",
    "a-stove0-nvenc-av1-opus-target",
    "a-stove0-opus-target",
    "a-review0-materializer",
    "a-review0-rclone-target",
    "a-riverhog-event-relay",
    "test",
  ]
}

// Update a readable image version and its digest together, then run `make build`.
target "image-common" {
  platforms = ["linux/amd64"]
  args = {
    SOURCE_DATE_EPOCH = "0"
  }
  attest = [
    "type=sbom,generator=docker.io/docker/buildkit-syft-scanner:stable-1@sha256:79e7b013cbec16bbb436f312819a49a4a57752b2270c1a9332ae1a10fcc82a68",
  ]
}

target "riverhog" {
  inherits   = ["image-common"]
  context    = "."
  dockerfile = "riverhog/Dockerfile"
  tags       = ["riverhog-app:dev"]
  args       = { SOURCE_REVISION = "unknown" }
}

target "a-riverhog-ftp-spool" {
  inherits   = ["image-common"]
  context    = "."
  dockerfile = "some-implementations/riverhog/ingress/ftp/Dockerfile"
  tags       = ["a-riverhog-ftp-spool:dev"]
  args       = { SOURCE_REVISION = "unknown" }
}

target "a-riverhog-aws-store" {
  inherits   = ["image-common"]
  context    = "."
  dockerfile = "some-implementations/riverhog/storage/aws/Dockerfile"
  tags       = ["a-riverhog-aws-store:dev"]
  args       = { SOURCE_REVISION = "unknown" }
}

target "a-riverhog-b2-store" {
  inherits   = ["image-common"]
  context    = "."
  dockerfile = "some-implementations/riverhog/storage/backblaze/Dockerfile"
  tags       = ["a-riverhog-b2-store:dev"]
  args       = { SOURCE_REVISION = "unknown" }
}

target "a-riverhog-filesystem-store" {
  inherits   = ["image-common"]
  context    = "."
  dockerfile = "some-implementations/riverhog/storage/filesystem/Dockerfile"
  tags       = ["a-riverhog-filesystem-store:dev"]
  args       = { SOURCE_REVISION = "unknown" }
}

target "stove0" {
  inherits   = ["image-common"]
  context    = "."
  dockerfile = "some-implementations/stove0/application/server/Dockerfile"
  tags       = ["stove0:dev"]
  args       = { SOURCE_REVISION = "unknown" }
}

target "a-stove0-exiftool-observer" {
  inherits   = ["image-common"]
  context    = "."
  dockerfile = "some-implementations/stove0/observers/exiftool/Dockerfile"
  tags       = ["a-stove0-exiftool-observer:dev"]
  args       = { SOURCE_REVISION = "unknown" }
}

target "a-stove0-ffprobe-sampling-observer" {
  inherits   = ["image-common"]
  context    = "."
  dockerfile = "some-implementations/stove0/observers/ffprobe-sampling/Dockerfile"
  tags       = ["a-stove0-ffprobe-sampling-observer:dev"]
  args       = { SOURCE_REVISION = "unknown" }
}

target "a-stove0-nvenc-av1-opus-target" {
  inherits   = ["image-common"]
  context    = "."
  dockerfile = "some-implementations/stove0/targets/nvenc-av1-opus/Dockerfile"
  tags       = ["a-stove0-nvenc-av1-opus-target:dev"]
  args       = { SOURCE_REVISION = "unknown" }
}

target "a-stove0-opus-target" {
  inherits   = ["image-common"]
  context    = "."
  dockerfile = "some-implementations/stove0/targets/opus/Dockerfile"
  tags       = ["a-stove0-opus-target:dev"]
  args       = { SOURCE_REVISION = "unknown" }
}

target "a-review0-materializer" {
  inherits   = ["image-common"]
  context    = "."
  dockerfile = "some-implementations/stove0/review0/materialize-target/Dockerfile"
  tags       = ["a-review0-materializer:dev"]
  args       = { SOURCE_REVISION = "unknown" }
}

target "a-review0-rclone-target" {
  inherits   = ["image-common"]
  context    = "."
  dockerfile = "some-implementations/stove0/review0/rclone-effect-target/Dockerfile"
  tags       = ["a-review0-rclone-target:dev"]
  args       = { SOURCE_REVISION = "unknown" }
}

target "a-riverhog-event-relay" {
  inherits   = ["image-common"]
  context    = "."
  dockerfile = "some-implementations/riverhog/applications/a-riverhog-event-relay/Dockerfile"
  tags       = ["a-riverhog-event-relay:dev"]
  args       = { SOURCE_REVISION = "unknown" }
}

target "test" {
  inherits   = ["image-common"]
  context    = "."
  dockerfile = "tests/Dockerfile"
  tags       = ["riverhog-test:dev"]
  args       = { SOURCE_REVISION = "unknown" }
}
