# lifecycle-events

[Atlas](../../index.md) · [Relationships](../index.md) · [Components](index.md)

Durable CloudEvents lifecycle log and client primitives.

| Boundary field | Value |
|---|---|
| Release role | `reusable_library` |
| Source path | `packages/lifecycle-events` |
| Description source | `packages/lifecycle-events/pyproject.toml#/project/description` |
| Owned contract elements | 2 |

[Open exact owned authority](../../authorities/lifecycle-events/index.md)

## Typed relationships

| Direction | Relationship | Counterparty | Scope or binding |
|---|---|---|---|
| outgoing | `depends-on` | [time-formats](time-formats.md) | `required` |
| incoming | `depends-on` | [mango-fish](mango-fish.md) | `required` |
| incoming | `depends-on` | [riverhog-protocol](riverhog-protocol.md) | `required` |
| incoming | `depends-on` | [riverhog-server](riverhog-server.md) | `required` |
| incoming | `depends-on` | [stove0-operator-contracts](stove0-operator-contracts.md) | `required` |
