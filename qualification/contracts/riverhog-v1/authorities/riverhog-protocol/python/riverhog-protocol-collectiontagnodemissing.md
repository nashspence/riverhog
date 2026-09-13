# riverhog_protocol.CollectionTagNodeMissing

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-collectiontagnodemissing:2e868a62f8 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-474369dd40"></a>
| Field | Shape |
|---|---|
| <a id="s-7e5982710c"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-0e576bdc91"></a>`distribution` | "riverhog-protocol" |
| <a id="s-aaf37b6932"></a>`module` | "riverhog_protocol" |
| <a id="s-0da906cc6e"></a>`name` | "CollectionTagNodeMissing" |
| <a id="s-f8ce1fabdd"></a>`unit` | "export" |

## Governing policies

- <a id="pa-3aaa14267c"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources.md#src-19e35f15d9) — `packages/riverhog-protocol/src/riverhog_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_protocol.CollectionTagNodeMissing`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 3da7399739d32593429ccaf8024239ffb3d9870bf71901a3b787bf9bc3ce62a2 -->

```json
{
  "contract": {
    "kind": "class",
    "signature": "unavailable"
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "CollectionTagNodeMissing",
  "unit": "export"
}
```
