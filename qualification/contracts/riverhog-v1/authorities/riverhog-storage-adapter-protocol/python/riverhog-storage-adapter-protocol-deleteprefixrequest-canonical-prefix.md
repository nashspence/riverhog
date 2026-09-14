# riverhog_storage_adapter_protocol.DeletePrefixRequest.canonical_prefix

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-storage-adapter-protocol:riverhog-storage-adapter-protocol-deletep-0cebf4b607:28268d8184 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-9a65d46f9f"></a>
- <a id="s-61659488e3"></a>`distribution`: `riverhog-storage-adapter-protocol`
- <a id="s-c9fccf7f39"></a>`module`: `riverhog_storage_adapter_protocol`
- <a id="s-46b204e6ad"></a>`name`: `canonical_prefix`
- <a id="s-47177bc289"></a>`owner`: `riverhog_storage_adapter_protocol.DeletePrefixRequest`
- <a id="s-48396d1bd9"></a>`unit`: `member`

### Declared structure

- <a id="s-9b153f6876"></a>`kind`: `"classmethod"`
- <a id="s-52cfed511e"></a>`signature`: `"\"(cls, value: 'str') -> 'str'\""`

## Maintained corroboration

### Related interface records

- [riverhog_storage_adapter_protocol.DeletePrefixRequest](riverhog-storage-adapter-protocol-deleteprefixrequest.md)

## Governing policies

- <a id="pa-1fa42237f2"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-storage-adapter-protocol:riverhog_storage_adapter_protocol](../../../evidence/sources.md#src-2da8857a83) — `packages/riverhog-storage-adapter-protocol/src/riverhog_storage_adapter_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_storage_adapter_protocol.DeletePrefixRequest.canonical_prefix`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: a78933139359a0fc39b90e2c86d6731fb8ec92604a0e452d4afac3730b93ec13 -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, value: 'str') -> 'str'\""
  },
  "distribution": "riverhog-storage-adapter-protocol",
  "module": "riverhog_storage_adapter_protocol",
  "name": "canonical_prefix",
  "owner": "riverhog_storage_adapter_protocol.DeletePrefixRequest",
  "unit": "member"
}
```
