# riverhog_storage_adapter_protocol.WriteStartRequest.canonical_path

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-storage-adapter-protocol:riverhog-storage-adapter-protocol-writest-70c4094666:661ccff116 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-170b624209"></a>
| Field | Shape |
|---|---|
| <a id="s-458b10a965"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-75b3451d5c"></a>`distribution` | "riverhog-storage-adapter-protocol" |
| <a id="s-94fef6b9db"></a>`module` | "riverhog_storage_adapter_protocol" |
| <a id="s-58f356f293"></a>`name` | "canonical_path" |
| <a id="s-9832f74697"></a>`owner` | "riverhog_storage_adapter_protocol.WriteStartRequest" |
| <a id="s-267fbfcd28"></a>`unit` | "member" |

## Maintained corroboration

### Related interface records

- [riverhog_storage_adapter_protocol.WriteStartRequest](riverhog-storage-adapter-protocol-writestartrequest.md)

## Governing policies

- <a id="pa-2f225e58a4"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-storage-adapter-protocol:riverhog_storage_adapter_protocol](../../../evidence/sources.md#src-2da8857a83) — `packages/riverhog-storage-adapter-protocol/src/riverhog_storage_adapter_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_storage_adapter_protocol.WriteStartRequest.canonical_path`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 463cb1005e7a571d464981c26e0a80798e4f463e84d381bd55c2aa65f09d8a71 -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, value: 'str') -> 'str'\""
  },
  "distribution": "riverhog-storage-adapter-protocol",
  "module": "riverhog_storage_adapter_protocol",
  "name": "canonical_path",
  "owner": "riverhog_storage_adapter_protocol.WriteStartRequest",
  "unit": "member"
}
```
