# riverhog_archive_contracts.ArchiveVolume

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-archive-contracts:riverhog-archive-contracts-archivevolume:6fb15de457 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-archive-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-b41db5c664"></a>
| Field | Shape |
|---|---|
| <a id="s-f021ee0b22"></a>`contract` | type="types.UnionType"; additional keys=`kind` |
| <a id="s-2bfa99c48a"></a>`distribution` | "riverhog-archive-contracts" |
| <a id="s-500af61d7a"></a>`module` | "riverhog_archive_contracts" |
| <a id="s-1a587c779c"></a>`name` | "ArchiveVolume" |
| <a id="s-b62471df83"></a>`unit` | "export" |

## Governing policies

- <a id="pa-da991680d6"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-archive-contracts:riverhog_archive_contracts](../../../evidence/sources.md#src-4557222ddc) — `packages/riverhog-archive-contracts/src/riverhog_archive_contracts/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_archive_contracts.ArchiveVolume`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 0fec1805cbe2632f8a9bc06abc47b1f35c78c87058618dc4d42474bfba5bc4c5 -->

```json
{
  "contract": {
    "kind": "object",
    "type": "types.UnionType"
  },
  "distribution": "riverhog-archive-contracts",
  "module": "riverhog_archive_contracts",
  "name": "ArchiveVolume",
  "unit": "export"
}
```
