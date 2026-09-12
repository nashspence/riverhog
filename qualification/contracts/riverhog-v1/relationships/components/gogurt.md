# gogurt

[Atlas](../../index.md) · [Relationships](../index.md) · [Components](index.md)

Optional nonnormative mounted-volume ingestion reference application for Riverhog.

| Boundary field | Value |
|---|---|
| Release role | `reference_application` |
| Source path | `reference/gogurt/application` |
| Description source | `reference/gogurt/application/pyproject.toml#/project/description` |
| Owned contract elements | 22 |

[Open exact owned authority](../../authorities/gogurt/index.md)

## Typed relationships

| Direction | Relationship | Counterparty | Scope or binding |
|---|---|---|---|
| outgoing | `depends-on` | [config-validation](config-validation.md) | `required` |
| outgoing | `depends-on` | [gogurt-core](gogurt-core.md) | `required` |
| outgoing | `depends-on` | [gogurt-listener-runtime](gogurt-listener-runtime.md) | `required` |
| outgoing | `installed-as` | [uv-tool](../installation/index.md#node-installation-uv-tool) | `` |
