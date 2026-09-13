# riverhog_storage_adapter_protocol.ReadPreparationRequest.canonical_objects

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-storage-adapter-protocol:riverhog-storage-adapter-protocol-readpre-18eb539f74:a6344141ae -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-61453d75dd"></a>
| Field | Shape |
|---|---|
| <a id="s-8f8ebe8b6b"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-d1fe00ccfb"></a>`distribution` | "riverhog-storage-adapter-protocol" |
| <a id="s-9904f5b78e"></a>`module` | "riverhog_storage_adapter_protocol" |
| <a id="s-45ebc30570"></a>`name` | "canonical_objects" |
| <a id="s-8280fa8bde"></a>`owner` | "riverhog_storage_adapter_protocol.ReadPreparationRequest" |
| <a id="s-401922735c"></a>`unit` | "member" |

## Maintained corroboration

### Related interface records

- [riverhog_storage_adapter_protocol.ReadPreparationRequest](riverhog-storage-adapter-protocol-readpreparationrequest.md)

## Governing policies

- <a id="pa-44a4505875"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-storage-adapter-protocol:riverhog_storage_adapter_protocol](../../../evidence/sources.md#src-2da8857a83) — `packages/riverhog-storage-adapter-protocol/src/riverhog_storage_adapter_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_storage_adapter_protocol.ReadPreparationRequest.canonical_objects`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: a2a1115e24cacb6a5b955dc6d2481e4ad56d3d65fa2f00c77ba9fffb4eb89b8a -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, value: 'tuple[ObjectLocator, ...]') -> 'tuple[ObjectLocator, ...]'\""
  },
  "distribution": "riverhog-storage-adapter-protocol",
  "module": "riverhog_storage_adapter_protocol",
  "name": "canonical_objects",
  "owner": "riverhog_storage_adapter_protocol.ReadPreparationRequest",
  "unit": "member"
}
```
