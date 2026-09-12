# Maintainer-selected Riverhog references

[Atlas](../index.md)

Checked-in references form a closed, tightly scoped, maintainer-selected, nonnormative conformance set.

## Surface shape

| Semantic area | Exact authorities | Contract elements |
|---|---:|---:|
| Riverhog references | 14 | 68 |
| Gogurt | 11 | 39 |
| Mango Fish | 2 | 8 |
| Piggity | 2 | 95 |

## Riverhog references

Authorities: **14** · Contract elements: **68**

| Exact authority | Contract elements | Interfaces | Maintained purpose |
|---|---:|---|---|
| [riverhog-ftp-adapter](../authorities/riverhog-ftp-adapter/index.md) | 29 | boundary, cli, configuration, configuration-environment, http, operation | Optional nonnormative FTP ingress reference for Riverhog. |
| [riverhog-ftp-adapter-api-client](../authorities/riverhog-ftp-adapter-api-client/index.md) | 6 | boundary, configuration-environment | Optional nonnormative client for the Riverhog FTP ingress reference. |
| [riverhog-ftp-custody](../authorities/riverhog-ftp-custody/index.md) | 1 | durable-state | Optional nonnormative FTP ingress reference for Riverhog. |
| [riverhog-provenance-linux-contracts](../authorities/riverhog-provenance-linux-contracts/index.md) | 6 | boundary, protocol | Optional nonnormative Linux observation-contract reference for Riverhog provenance. |
| [riverhog-provenance-linux-observer](../authorities/riverhog-provenance-linux-observer/index.md) | 1 | boundary | Optional nonnormative Linux filesystem-observer reference for Riverhog provenance. |
| [riverhog-provenance-macos-contracts](../authorities/riverhog-provenance-macos-contracts/index.md) | 5 | boundary, protocol | Optional nonnormative macOS observation-contract reference for Riverhog provenance. |
| [riverhog-provenance-macos-observer](../authorities/riverhog-provenance-macos-observer/index.md) | 1 | boundary | Optional nonnormative macOS filesystem-observer reference for Riverhog provenance. |
| [riverhog-provenance-windows-contracts](../authorities/riverhog-provenance-windows-contracts/index.md) | 11 | boundary, protocol | Optional nonnormative Windows observation-contract reference for Riverhog provenance. |
| [riverhog-provenance-windows-observer](../authorities/riverhog-provenance-windows-observer/index.md) | 1 | boundary | Optional nonnormative Windows filesystem-observer reference for Riverhog provenance. |
| [riverhog-recover](../authorities/riverhog-recover/index.md) | 2 | boundary, cli | Optional nonnormative independent recovery reference application for Riverhog archives. |
| [riverhog-storage-adapter-aws](../authorities/riverhog-storage-adapter-aws/index.md) | 1 | boundary | Optional nonnormative AWS storage reference for Riverhog. |
| [riverhog-storage-adapter-backblaze](../authorities/riverhog-storage-adapter-backblaze/index.md) | 1 | boundary | Optional nonnormative Backblaze B2 storage reference for Riverhog. |
| [riverhog-storage-adapter-filesystem](../authorities/riverhog-storage-adapter-filesystem/index.md) | 2 | boundary, cli | Optional nonnormative Linux filesystem storage reference for Riverhog. |
| [riverhog-storage-adapter-s3-support](../authorities/riverhog-storage-adapter-s3-support/index.md) | 1 | boundary | Optional nonnormative S3 support for Riverhog storage references. |

## Gogurt

Authorities: **11** · Contract elements: **39**

| Exact authority | Contract elements | Interfaces | Maintained purpose |
|---|---:|---|---|
| [gogurt](../authorities/gogurt/index.md) | 24 | boundary, cli, configuration-environment | Optional nonnormative mounted-volume ingestion reference application for Riverhog. |
| [gogurt-core](../authorities/gogurt-core/index.md) | 4 | boundary, configuration, python | Portable Gogurt marker, routing, action, and watch semantics. |
| [gogurt-linux-listener-host](../authorities/gogurt-linux-listener-host/index.md) | 1 | boundary | Optional nonnormative Linux systemd-user listener-host reference for Gogurt. |
| [gogurt-linux-mounted-volume](../authorities/gogurt-linux-mounted-volume/index.md) | 1 | boundary | Optional nonnormative Linux mounted-volume reference for Gogurt. |
| [gogurt-listener](../authorities/gogurt-listener/index.md) | 1 | durable-state | Portable durable listener runtime and native-platform port for Gogurt. |
| [gogurt-listener-runtime](../authorities/gogurt-listener-runtime/index.md) | 3 | boundary, python | Portable durable listener runtime and native-platform port for Gogurt. |
| [gogurt-macos-listener-host](../authorities/gogurt-macos-listener-host/index.md) | 1 | boundary | Optional nonnormative macOS launchd listener-host reference for Gogurt. |
| [gogurt-macos-mounted-volume](../authorities/gogurt-macos-mounted-volume/index.md) | 1 | boundary | Optional nonnormative macOS mounted-volume reference for Gogurt. |
| [gogurt-path-volume-support](../authorities/gogurt-path-volume-support/index.md) | 1 | boundary | Optional nonnormative path-mounted-volume support for Gogurt reference providers. |
| [gogurt-windows-listener-host](../authorities/gogurt-windows-listener-host/index.md) | 1 | boundary | Optional nonnormative Windows Task Scheduler listener-host reference for Gogurt. |
| [gogurt-windows-mounted-volume](../authorities/gogurt-windows-mounted-volume/index.md) | 1 | boundary | Optional nonnormative Windows mounted-volume reference for Gogurt. |

## Mango Fish

Authorities: **2** · Contract elements: **8**

| Exact authority | Contract elements | Interfaces | Maintained purpose |
|---|---:|---|---|
| [mango-fish](../authorities/mango-fish/index.md) | 7 | boundary, cli, configuration | Optional nonnormative CloudEvents reference application for Riverhog. |
| [mango-fish-cursor](../authorities/mango-fish-cursor/index.md) | 1 | durable-state | Optional nonnormative CloudEvents reference application for Riverhog. |

## Piggity

Authorities: **2** · Contract elements: **95**

| Exact authority | Contract elements | Interfaces | Maintained purpose |
|---|---:|---|---|
| [piggity](../authorities/piggity/index.md) | 94 | boundary, cli, configuration-environment | Optional nonnormative Piggity reference client for Riverhog. |
| [piggity-local](../authorities/piggity-local/index.md) | 1 | durable-state | Optional nonnormative Piggity reference client for Riverhog. |
