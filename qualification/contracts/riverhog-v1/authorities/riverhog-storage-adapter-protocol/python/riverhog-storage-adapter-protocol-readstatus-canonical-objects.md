# riverhog_storage_adapter_protocol.ReadStatus.canonical_objects

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-storage-adapter-protocol:riverhog-storage-adapter-protocol-readsta-44fb3df56a:ad99a03840 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-8006b31287"></a>
- <a id="s-5632f840cf"></a>`distribution`: `riverhog-storage-adapter-protocol`
- <a id="s-038b98a971"></a>`module`: `riverhog_storage_adapter_protocol`
- <a id="s-2a73a08cb0"></a>`name`: `canonical_objects`
- <a id="s-b6cc127397"></a>`owner`: `riverhog_storage_adapter_protocol.ReadStatus`
- <a id="s-2abe38be91"></a>`unit`: `member`

### Declared structure

- <a id="s-cb694add07"></a>`kind`: `"classmethod"`
- <a id="s-f8dffcfe61"></a>`signature`: `"\"(cls, value: 'tuple[ObjectLocator, ...]') -> 'tuple[ObjectLocator, ...]'\""`

## Maintained corroboration

### Related interface records

- [ReadStatus](riverhog-storage-adapter-protocol-readstatus.md)

## Governing policies

- <a id="pa-0c4d978f18"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-storage-adapter-protocol:riverhog_storage_adapter_protocol](../../../evidence/sources.md#src-2da8857a83) — `packages/riverhog-storage-adapter-protocol/src/riverhog_storage_adapter_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_storage_adapter_protocol.ReadStatus.canonical_objects`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 4a3ad7e496900c77174a6f897441868cd74d37343e020fab6bb331fe1553ca68 -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, value: 'tuple[ObjectLocator, ...]') -> 'tuple[ObjectLocator, ...]'\""
  },
  "distribution": "riverhog-storage-adapter-protocol",
  "module": "riverhog_storage_adapter_protocol",
  "name": "canonical_objects",
  "owner": "riverhog_storage_adapter_protocol.ReadStatus",
  "unit": "member"
}
```

</details>
