# gogurt-linux-mounted-volume

[Atlas](../../index.md) · [Relationships](../index.md) · [Components](index.md)

Optional nonnormative Linux mounted-volume reference for Gogurt.

| Boundary field | Value |
|---|---|
| Release role | `reference_component` |
| Source path | `reference/gogurt/mounted-volume/linux` |
| Description source | `reference/gogurt/mounted-volume/linux/pyproject.toml#/project/description` |
| Owned contract elements | 1 |

[Open exact owned authority](../../authorities/gogurt-linux-mounted-volume/index.md)

## Typed relationships

| Direction | Relationship | Counterparty | Scope or binding |
|---|---|---|---|
| outgoing | `depends-on` | [gogurt-core](gogurt-core.md) | `required` |
| outgoing | `depends-on` | [gogurt-path-volume-support](gogurt-path-volume-support.md) | `required` |
| outgoing | `implements-extension-point` | [gogurt.mounted-volume-providers](../extensions/index.md#node-extension-point-gogurt-mounted-volume-providers) | `gogurt-linux-mounted-volume` |
