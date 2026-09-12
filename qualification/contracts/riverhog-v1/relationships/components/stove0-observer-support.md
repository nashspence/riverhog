# stove0-observer-support

[Atlas](../../index.md) · [Relationships](../index.md) · [Components](index.md)

External-author protocol, runtime, and conformance support for stove0 content observers.

| Boundary field | Value |
|---|---|
| Release role | `reusable_library` |
| Source path | `reference/stove0/packages/observer-support` |
| Description source | `reference/stove0/packages/observer-support/pyproject.toml#/project/description` |
| Owned contract elements | 10 |

[Open exact owned authority](../../authorities/stove0-observer-support/index.md)

## Typed relationships

| Direction | Relationship | Counterparty | Scope or binding |
|---|---|---|---|
| outgoing | `binds-protocol` | [stove0-observer](../extensions/index.md#node-process-protocol-stove0-observer) | `http` |
| outgoing | `depends-on` | [http-api-contracts](http-api-contracts.md) | `required` |
| outgoing | `depends-on` | [riverhog-client](riverhog-client.md) | `required` |
| outgoing | `depends-on` | [stove0-observer-client](stove0-observer-client.md) | `required` |
| outgoing | `depends-on` | [stove0-observer-protocol](stove0-observer-protocol.md) | `required` |
| incoming | `depends-on` | [stove0-exiftool-observer](stove0-exiftool-observer.md) | `required` |
| incoming | `depends-on` | [stove0-ffprobe-sampling-observer](stove0-ffprobe-sampling-observer.md) | `required` |
