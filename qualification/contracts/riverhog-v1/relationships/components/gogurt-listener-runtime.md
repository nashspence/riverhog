# gogurt-listener-runtime

[Atlas](../../index.md) · [Relationships](../index.md) · [Components](index.md)

Portable durable listener runtime and native-platform port for Gogurt.

| Boundary field | Value |
|---|---|
| Release role | `reusable_library` |
| Source path | `reference/gogurt/packages/listener-runtime` |
| Description source | `reference/gogurt/packages/listener-runtime/pyproject.toml#/project/description` |
| Owned contract elements | 3 |

[Open exact owned authority](../../authorities/gogurt-listener-runtime/index.md)

## Typed relationships

| Direction | Relationship | Counterparty | Scope or binding |
|---|---|---|---|
| outgoing | `depends-on` | [config-validation](config-validation.md) | `required` |
| outgoing | `depends-on` | [gogurt-core](gogurt-core.md) | `required` |
| outgoing | `owns-extension-point` | [gogurt.listener-host-providers](../extensions/index.md#node-extension-point-gogurt-listener-host-providers) | `` |
| incoming | `depends-on` | [gogurt](gogurt.md) | `required` |
| incoming | `depends-on` | [gogurt-linux-listener-host](gogurt-linux-listener-host.md) | `required` |
| incoming | `depends-on` | [gogurt-macos-listener-host](gogurt-macos-listener-host.md) | `required` |
| incoming | `depends-on` | [gogurt-windows-listener-host](gogurt-windows-listener-host.md) | `required` |
