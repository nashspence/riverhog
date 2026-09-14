# riverhog_storage_adapter_protocol.WriteSession.canonical_path

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-storage-adapter-protocol:riverhog-storage-adapter-protocol-writese-332b86410b:d4d0fbcb04 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-adf6a6e1ab"></a>
- <a id="s-76ec1b7766"></a>`distribution`: `riverhog-storage-adapter-protocol`
- <a id="s-8a4e0ad8b1"></a>`module`: `riverhog_storage_adapter_protocol`
- <a id="s-f20bfc5fee"></a>`name`: `canonical_path`
- <a id="s-762a1d3934"></a>`owner`: `riverhog_storage_adapter_protocol.WriteSession`
- <a id="s-c426685305"></a>`unit`: `member`

### Declared structure

- <a id="s-5043020cac"></a>`kind`: `"classmethod"`
- <a id="s-e6f1f89844"></a>`signature`: `"\"(cls, value: 'str') -> 'str'\""`

## Maintained corroboration

### Related interface records

- [riverhog_storage_adapter_protocol.WriteSession](riverhog-storage-adapter-protocol-writesession.md)

## Governing policies

- <a id="pa-eb947a3cf1"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-storage-adapter-protocol:riverhog_storage_adapter_protocol](../../../evidence/sources.md#src-2da8857a83) — `packages/riverhog-storage-adapter-protocol/src/riverhog_storage_adapter_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_storage_adapter_protocol.WriteSession.canonical_path`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 1900eb91755974715e4b95010104e492367b0cc6263809a82857b191d42699d4 -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, value: 'str') -> 'str'\""
  },
  "distribution": "riverhog-storage-adapter-protocol",
  "module": "riverhog_storage_adapter_protocol",
  "name": "canonical_path",
  "owner": "riverhog_storage_adapter_protocol.WriteSession",
  "unit": "member"
}
```
