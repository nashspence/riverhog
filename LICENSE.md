# Riverhog licensing

Riverhog uses a component-based licensing boundary. Implementations entrusted with
durable user content or durable user-owned evidence use CAL-1.0. Operational state
alone does not change a component's license.

The following implementations are licensed under the Cryptographic Autonomy
License 1.0 (`CAL-1.0`):

- `riverhog/**`
- `some-implementations/stove0/application/server/**`
- `some-implementations/riverhog/storage/aws/**`
- `some-implementations/riverhog/storage/backblaze/**`
- `some-implementations/riverhog/storage/filesystem/**`
- `some-implementations/riverhog/ingress/ftp/**`
- `some-implementations/riverhog/applications/a-riverhog-minisign-witness/**`
- `some-implementations/riverhog/applications/a-riverhog-opentimestamps-witness/**`
- `some-implementations/stove0/observers/exiftool/**`
- `some-implementations/stove0/observers/ffprobe-sampling/**`
- `some-implementations/stove0/targets/nvenc-av1-opus/**`
- `some-implementations/stove0/targets/opus/**`
- `some-implementations/stove0/review0/**`

Published OpenAPI, Review0 interoperability packages, and its operational planning
and sampler support packages beneath those directories are licensed under Apache
License 2.0 as recorded in `REUSE.toml`. Server
Dockerfiles, migrations, configuration schemas, deployment examples, and maintained
observer/target implementations remain CAL-licensed. No Riverhog file carries CAL's
Combined Work Exception unless it is explicitly identified there in the future.

All other first-party repository content is licensed under Apache License 2.0
(`Apache-2.0`) by default. This includes clients, reusable packages, other supplied applications,
documentation, scripts, tests, archive-format and protocol specifications, API clients,
SDKs, interoperability examples, and independent recovery tooling.

`REUSE.toml` is the machine-readable authority for individual paths. Full license texts are
in `LICENSES/`. Third-party components retain their own licenses; see the
[third-party notices](THIRD_PARTY_NOTICES.md).
