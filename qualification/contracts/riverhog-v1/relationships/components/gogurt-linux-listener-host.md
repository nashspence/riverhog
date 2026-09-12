# gogurt-linux-listener-host

[Atlas](../../index.md) · [Relationships](../index.md) · [Components](index.md)

Optional nonnormative Linux systemd-user listener-host reference for Gogurt.

| Boundary field | Value |
|---|---|
| Release role | `reference_component` |
| Source path | `reference/gogurt/listener-host/linux` |
| Description source | `reference/gogurt/listener-host/linux/pyproject.toml#/project/description` |
| Owned contract elements | 1 |

[Open exact owned authority](../../authorities/gogurt-linux-listener-host/index.md)

## Typed relationships

| Direction | Relationship | Counterparty | Scope or binding |
|---|---|---|---|
| outgoing | `depends-on` | [gogurt-listener-runtime](gogurt-listener-runtime.md) | `required` |
| outgoing | `implements-extension-point` | [gogurt.listener-host-providers](../extensions/index.md#node-extension-point-gogurt-listener-host-providers) | `gogurt-linux-listener-host` |
