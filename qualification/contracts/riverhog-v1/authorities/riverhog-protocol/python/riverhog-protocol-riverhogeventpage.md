# riverhog_protocol.RiverhogEventPage

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-riverhogeventpage:b756e6c28e -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-34f582a2ca"></a>
| Field | Shape |
|---|---|
| <a id="s-5e7b360414"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-70c2b349dc"></a>`distribution` | "riverhog-protocol" |
| <a id="s-b261a446ec"></a>`module` | "riverhog_protocol" |
| <a id="s-f0b2a3acab"></a>`name` | "RiverhogEventPage" |
| <a id="s-79f392f105"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [riverhog_protocol.RiverhogEventPage.require_progress_after](riverhog-protocol-riverhogeventpage-require-progress-after.md)

## Governing policies

- <a id="pa-962d15f316"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources.md#src-19e35f15d9) — `packages/riverhog-protocol/src/riverhog_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_protocol.RiverhogEventPage`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 6d27999470ab42eefbeb42ab9d51d882d6f757091f1e1bc21c8a4559337d1cee -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "85667d68d6f7d8fd2b316d87e12db272005e8852005d452205f699020588ab62",
    "signature": "'(*, events: list[RiverhogLifecycleEvent], next_cursor: LifecycleEventCursor, has_more: bool) -> None'"
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "RiverhogEventPage",
  "unit": "export"
}
```
