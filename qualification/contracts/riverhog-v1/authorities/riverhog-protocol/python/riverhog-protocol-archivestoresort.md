# riverhog_protocol.ArchiveStoreSort

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-archivestoresort:a5e40b3f07 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-d7c84627dc"></a>
| Field | Shape |
|---|---|
| <a id="s-9cb44513f1"></a>`contract` | additional keys=`kind`, `value` |
| <a id="s-666c18d676"></a>`distribution` | "riverhog-protocol" |
| <a id="s-ac892897a3"></a>`module` | "riverhog_protocol" |
| <a id="s-11587834eb"></a>`name` | "ArchiveStoreSort" |
| <a id="s-d3525e3dfd"></a>`unit` | "export" |

## Governing policies

- <a id="pa-380b52c51e"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources.md#src-19e35f15d9) — `packages/riverhog-protocol/src/riverhog_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_protocol.ArchiveStoreSort`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 1d0b4fa78c2b55080286d598c44625b1f56fff7ce0fc9b38a475bc0bdbce62f2 -->

```json
{
  "contract": {
    "kind": "type-alias",
    "value": "typing.Literal['store', 'read_mode', 'read_priority', 'collections', 'objects', 'stored_bytes']"
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "ArchiveStoreSort",
  "unit": "export"
}
```
