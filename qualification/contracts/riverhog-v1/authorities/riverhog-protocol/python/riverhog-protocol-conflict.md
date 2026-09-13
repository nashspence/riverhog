# riverhog_protocol.Conflict

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-conflict:119a6c8729 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-3ea9527f1d"></a>
| Field | Shape |
|---|---|
| <a id="s-57a4c1c7f5"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-14291b1de6"></a>`distribution` | "riverhog-protocol" |
| <a id="s-c8b26bd490"></a>`module` | "riverhog_protocol" |
| <a id="s-4ba0b098ed"></a>`name` | "Conflict" |
| <a id="s-c99c1209df"></a>`unit` | "export" |

## Governing policies

- <a id="pa-3d9911768b"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources.md#src-19e35f15d9) — `packages/riverhog-protocol/src/riverhog_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_protocol.Conflict`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 538cf0202b32db5d148b071bb4d673bb232578027cafbf7e3809e2cdec58aab9 -->

```json
{
  "contract": {
    "kind": "class",
    "signature": "\"(message: 'str', *, code: 'str | None' = None, observed_status: 'int | None' = None, details: 'dict[str, Any] | None' = None) -> 'None'\""
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "Conflict",
  "unit": "export"
}
```
