# gogurt-core

[Atlas](../../index.md) · [Relationships](../index.md) · [Components](index.md)

Portable Gogurt marker, routing, action, and watch semantics.

| Boundary field | Value |
|---|---|
| Release role | `reusable_library` |
| Source path | `reference/gogurt/packages/core` |
| Description source | `reference/gogurt/packages/core/pyproject.toml#/project/description` |
| Owned contract elements | 3 |

[Open exact owned authority](../../authorities/gogurt-core/index.md)

## Typed relationships

| Direction | Relationship | Counterparty | Scope or binding |
|---|---|---|---|
| outgoing | `depends-on` | [config-validation](config-validation.md) | `required` |
| outgoing | `owns-extension-point` | [gogurt.mounted-volume-providers](../extensions/index.md#node-extension-point-gogurt-mounted-volume-providers) | `` |
| incoming | `depends-on` | [gogurt](gogurt.md) | `required` |
| incoming | `depends-on` | [gogurt-linux-mounted-volume](gogurt-linux-mounted-volume.md) | `required` |
| incoming | `depends-on` | [gogurt-listener-runtime](gogurt-listener-runtime.md) | `required` |
| incoming | `depends-on` | [gogurt-macos-mounted-volume](gogurt-macos-mounted-volume.md) | `required` |
| incoming | `depends-on` | [gogurt-path-volume-support](gogurt-path-volume-support.md) | `required` |
| incoming | `depends-on` | [gogurt-windows-mounted-volume](gogurt-windows-mounted-volume.md) | `required` |
