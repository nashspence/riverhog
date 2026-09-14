# riverhog_storage_adapter_protocol.SmallObjectWriteRequest.canonical_metadata

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-storage-adapter-protocol:riverhog-storage-adapter-protocol-smallob-0cb42612fa:4301977e19 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-be1d3c3e96"></a>
- <a id="s-9fa7067793"></a>`distribution`: `riverhog-storage-adapter-protocol`
- <a id="s-6bf10b24f6"></a>`module`: `riverhog_storage_adapter_protocol`
- <a id="s-d048cac582"></a>`name`: `canonical_metadata`
- <a id="s-040082493d"></a>`owner`: `riverhog_storage_adapter_protocol.SmallObjectWriteRequest`
- <a id="s-8df3efd6d1"></a>`unit`: `member`

### Declared structure

- <a id="s-9692c1c407"></a>`kind`: `"classmethod"`
- <a id="s-8718a0c8c2"></a>`signature`: `"\"(cls, value: 'dict[str, str]') -> 'dict[str, str]'\""`

## Maintained corroboration

### Related interface records

- [riverhog_storage_adapter_protocol.SmallObjectWriteRequest](riverhog-storage-adapter-protocol-smallobjectwriterequest.md)

## Governing policies

- <a id="pa-7421bc28ce"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-storage-adapter-protocol:riverhog_storage_adapter_protocol](../../../evidence/sources.md#src-2da8857a83) — `packages/riverhog-storage-adapter-protocol/src/riverhog_storage_adapter_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_storage_adapter_protocol.SmallObjectWriteRequest.canonical_metadata`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 192573516bd665d7893ba513dac7126950cb4c986cb3212db90746b6f3adbd06 -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, value: 'dict[str, str]') -> 'dict[str, str]'\""
  },
  "distribution": "riverhog-storage-adapter-protocol",
  "module": "riverhog_storage_adapter_protocol",
  "name": "canonical_metadata",
  "owner": "riverhog_storage_adapter_protocol.SmallObjectWriteRequest",
  "unit": "member"
}
```
