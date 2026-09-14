# riverhog_storage_adapter_protocol.CompletedWriteLookupRequest.canonical_metadata

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-storage-adapter-protocol:riverhog-storage-adapter-protocol-complet-9f6ebd07d8:7f6738a25d -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-3b52ba51b5"></a>
- <a id="s-d79f0bf8f6"></a>`distribution`: `riverhog-storage-adapter-protocol`
- <a id="s-0b44d81e50"></a>`module`: `riverhog_storage_adapter_protocol`
- <a id="s-cda02f9361"></a>`name`: `canonical_metadata`
- <a id="s-663ed447a5"></a>`owner`: `riverhog_storage_adapter_protocol.CompletedWriteLookupRequest`
- <a id="s-eee465a423"></a>`unit`: `member`

### Declared structure

- <a id="s-4ef92e106e"></a>`kind`: `"classmethod"`
- <a id="s-884bd313eb"></a>`signature`: `"\"(cls, value: 'dict[str, str]') -> 'dict[str, str]'\""`

## Maintained corroboration

### Related interface records

- [riverhog_storage_adapter_protocol.CompletedWriteLookupRequest](riverhog-storage-adapter-protocol-completedwritelookuprequest.md)

## Governing policies

- <a id="pa-c3062fdc87"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-storage-adapter-protocol:riverhog_storage_adapter_protocol](../../../evidence/sources.md#src-2da8857a83) — `packages/riverhog-storage-adapter-protocol/src/riverhog_storage_adapter_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_storage_adapter_protocol.CompletedWriteLookupRequest.canonical_metadata`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 0bb8fc505fb57cf7e13c97ed3a62232bedab9f45614a5bc11ba275d664256c83 -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, value: 'dict[str, str]') -> 'dict[str, str]'\""
  },
  "distribution": "riverhog-storage-adapter-protocol",
  "module": "riverhog_storage_adapter_protocol",
  "name": "canonical_metadata",
  "owner": "riverhog_storage_adapter_protocol.CompletedWriteLookupRequest",
  "unit": "member"
}
```
