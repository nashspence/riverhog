# gogurt-path-volume-support

[Atlas](../../index.md) · [Relationships](../index.md) · [Components](index.md)

Optional nonnormative path-mounted-volume support for Gogurt reference providers.

| Boundary field | Value |
|---|---|
| Release role | `reference_component` |
| Source path | `reference/gogurt/mounted-volume/path-support` |
| Description source | `reference/gogurt/mounted-volume/path-support/pyproject.toml#/project/description` |
| Owned contract elements | 1 |

[Open exact owned authority](../../authorities/gogurt-path-volume-support/index.md)

## Typed relationships

| Direction | Relationship | Counterparty | Scope or binding |
|---|---|---|---|
| outgoing | `depends-on` | [config-validation](config-validation.md) | `required` |
| outgoing | `depends-on` | [gogurt-core](gogurt-core.md) | `required` |
| incoming | `depends-on` | [gogurt-linux-mounted-volume](gogurt-linux-mounted-volume.md) | `required` |
| incoming | `depends-on` | [gogurt-macos-mounted-volume](gogurt-macos-mounted-volume.md) | `required` |
| incoming | `depends-on` | [gogurt-windows-mounted-volume](gogurt-windows-mounted-volume.md) | `required` |
