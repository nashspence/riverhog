# riverhog_storage_adapter_protocol.WriteStartRequest.canonical_metadata

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-storage-adapter-protocol:riverhog-storage-adapter-protocol-writest-6c1f5161a0:ee71de42b0 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-ed67fbff72"></a>
- <a id="s-c6cf56e1de"></a>`distribution`: `riverhog-storage-adapter-protocol`
- <a id="s-42e0b112f3"></a>`module`: `riverhog_storage_adapter_protocol`
- <a id="s-29c2b4aa84"></a>`name`: `canonical_metadata`
- <a id="s-f4558cb41c"></a>`owner`: `riverhog_storage_adapter_protocol.WriteStartRequest`
- <a id="s-f2c5a101f4"></a>`unit`: `member`

### Declared structure

- <a id="s-377c5d4e70"></a>`kind`: `"classmethod"`
- <a id="s-0e1814c972"></a>`signature`: `"\"(cls, value: 'dict[str, str]') -> 'dict[str, str]'\""`

## Maintained corroboration

### Related interface records

- [WriteStartRequest](riverhog-storage-adapter-protocol-writestartrequest.md)

## Governing policies

- <a id="pa-91213ccdbb"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-storage-adapter-protocol:riverhog_storage_adapter_protocol](../../../evidence/sources.md#src-2da8857a83) — `packages/riverhog-storage-adapter-protocol/src/riverhog_storage_adapter_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_storage_adapter_protocol.WriteStartRequest.canonical_metadata`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 2d61924b80e43c72b16337e3c84c95850d94859f2d227979e319b83dfd09aa41 -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, value: 'dict[str, str]') -> 'dict[str, str]'\""
  },
  "distribution": "riverhog-storage-adapter-protocol",
  "module": "riverhog_storage_adapter_protocol",
  "name": "canonical_metadata",
  "owner": "riverhog_storage_adapter_protocol.WriteStartRequest",
  "unit": "member"
}
```
