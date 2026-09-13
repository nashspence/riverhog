# riverhog_storage_adapter_protocol.ReadExpired

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-storage-adapter-protocol:riverhog-storage-adapter-protocol-readexpired:98d4f677bd -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-6b9cbbcc9f"></a>
| Field | Shape |
|---|---|
| <a id="s-6a3324f20b"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-c5489728a3"></a>`distribution` | "riverhog-storage-adapter-protocol" |
| <a id="s-61ef9f8be6"></a>`module` | "riverhog_storage_adapter_protocol" |
| <a id="s-0d9a6017ff"></a>`name` | "ReadExpired" |
| <a id="s-ff88e97bd4"></a>`unit` | "export" |

## Governing policies

- <a id="pa-12a93238ba"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-storage-adapter-protocol:riverhog_storage_adapter_protocol](../../../evidence/sources.md#src-2da8857a83) — `packages/riverhog-storage-adapter-protocol/src/riverhog_storage_adapter_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_storage_adapter_protocol.ReadExpired`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 4d654151f8dc06c02848901da875d312ac90b67a77cd5cf6d6573ed951b49d91 -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "3a7d5d1ad6056eb379318b3aba88c1d36fb5a6cfea9d6b2bb1675751cf4bd2c2",
    "signature": "\"(*, state: Literal['expired'] = 'expired') -> None\""
  },
  "distribution": "riverhog-storage-adapter-protocol",
  "module": "riverhog_storage_adapter_protocol",
  "name": "ReadExpired",
  "unit": "export"
}
```
