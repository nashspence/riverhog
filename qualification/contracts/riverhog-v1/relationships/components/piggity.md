# piggity

[Atlas](../../index.md) · [Relationships](../index.md) · [Components](index.md)

Optional nonnormative Piggity reference client for Riverhog.

| Boundary field | Value |
|---|---|
| Release role | `reference_application` |
| Source path | `reference/riverhog/applications/piggity` |
| Description source | `reference/riverhog/applications/piggity/pyproject.toml#/project/description` |
| Owned contract elements | 87 |

[Open exact owned authority](../../authorities/piggity/index.md)

## Typed relationships

| Direction | Relationship | Counterparty | Scope or binding |
|---|---|---|---|
| outgoing | `depends-on` | [http-api-contracts](http-api-contracts.md) | `required` |
| outgoing | `depends-on` | [riverhog-application-access](riverhog-application-access.md) | `required` |
| outgoing | `depends-on` | [riverhog-client](riverhog-client.md) | `required` |
| outgoing | `depends-on` | [riverhog-protocol](riverhog-protocol.md) | `required` |
| outgoing | `depends-on` | [riverhog-provenance](riverhog-provenance.md) | `required` |
| outgoing | `depends-on` | [state-schema](state-schema.md) | `required` |
| outgoing | `depends-on` | [time-formats](time-formats.md) | `required` |
| outgoing | `installed-as` | [uv-tool](../installation/index.md#node-installation-uv-tool) | `` |
