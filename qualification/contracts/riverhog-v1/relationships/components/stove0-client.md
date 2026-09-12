# stove0-client

[Atlas](../../index.md) · [Relationships](../index.md) · [Components](index.md)

Optional nonnormative command-line client for the Stove0 reference application.

| Boundary field | Value |
|---|---|
| Release role | `reference_application` |
| Source path | `reference/stove0/application/client` |
| Description source | `reference/stove0/application/client/pyproject.toml#/project/description` |
| Owned contract elements | 1 |

[Open exact owned authority](../../authorities/stove0-client/index.md)

## Typed relationships

| Direction | Relationship | Counterparty | Scope or binding |
|---|---|---|---|
| outgoing | `depends-on` | [stove0-api-client](stove0-api-client.md) | `required` |
| outgoing | `depends-on` | [stove0-protocol](stove0-protocol.md) | `required` |
| outgoing | `depends-on` | [stove0-recipe-config](stove0-recipe-config.md) | `required` |
| outgoing | `installed-as` | [uv-tool](../installation/index.md#node-installation-uv-tool) | `` |
