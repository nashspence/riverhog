# riverhog_storage_adapter_protocol.ReadReady.canonical_available_until

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-storage-adapter-protocol:riverhog-storage-adapter-protocol-readrea-599f241b4d:22a483263e -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-6fb165ddb3"></a>
- <a id="s-283e65fd50"></a>`distribution`: `riverhog-storage-adapter-protocol`
- <a id="s-75001f4653"></a>`module`: `riverhog_storage_adapter_protocol`
- <a id="s-7d697a2e1d"></a>`name`: `canonical_available_until`
- <a id="s-224bcd7396"></a>`owner`: `riverhog_storage_adapter_protocol.ReadReady`
- <a id="s-29470c1611"></a>`unit`: `member`

### Declared structure

- <a id="s-2135f4b7db"></a>`kind`: `"classmethod"`
- <a id="s-0408fc1dee"></a>`signature`: `"\"(cls, value: 'str \| None') -> 'str \| None'\""`

## Maintained corroboration

### Related interface records

- [riverhog_storage_adapter_protocol.ReadReady](riverhog-storage-adapter-protocol-readready.md)

## Governing policies

- <a id="pa-8d4ab5b86c"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-storage-adapter-protocol:riverhog_storage_adapter_protocol](../../../evidence/sources.md#src-2da8857a83) — `packages/riverhog-storage-adapter-protocol/src/riverhog_storage_adapter_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_storage_adapter_protocol.ReadReady.canonical_available_until`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: c80853a8d493d64975795643e26bbf65ec867ff2ed537fdfd0b70eadc61600ed -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, value: 'str | None') -> 'str | None'\""
  },
  "distribution": "riverhog-storage-adapter-protocol",
  "module": "riverhog_storage_adapter_protocol",
  "name": "canonical_available_until",
  "owner": "riverhog_storage_adapter_protocol.ReadReady",
  "unit": "member"
}
```
