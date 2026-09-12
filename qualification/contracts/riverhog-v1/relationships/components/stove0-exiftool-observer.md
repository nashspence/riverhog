# stove0-exiftool-observer

[Atlas](../../index.md) · [Relationships](../index.md) · [Components](index.md)

Optional nonnormative ExifTool observer reference for Stove0.

| Boundary field | Value |
|---|---|
| Release role | `reference_component` |
| Source path | `reference/stove0/observers/exiftool` |
| Description source | `reference/stove0/observers/exiftool/pyproject.toml#/project/description` |
| Owned contract elements | 1 |

[Open exact owned authority](../../authorities/stove0-exiftool-observer/index.md)

## Typed relationships

| Direction | Relationship | Counterparty | Scope or binding |
|---|---|---|---|
| outgoing | `depends-on` | [http-api-contracts](http-api-contracts.md) | `required` |
| outgoing | `depends-on` | [stove0-media-metadata-observer-contracts](stove0-media-metadata-observer-contracts.md) | `required` |
| outgoing | `depends-on` | [stove0-observer-protocol](stove0-observer-protocol.md) | `required` |
| outgoing | `depends-on` | [stove0-observer-support](stove0-observer-support.md) | `required` |
| outgoing | `implements-protocol` | [stove0-observer](../extensions/index.md#node-process-protocol-stove0-observer) | `` |
| outgoing | `packaged-in` | [stove0-exiftool-observer](../runtime-images/index.md#node-image-runtime-stove0-exiftool-observer) | `` |
