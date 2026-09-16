# riverhog_archive_contracts.RecoveryDescriptor.from_json_bytes

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-archive-contracts:riverhog-archive-contracts-recoverydescri-fa0bac0450:b7500a310e -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-archive-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-4a04f8c323"></a>
- <a id="s-d053cafcd7"></a>`distribution`: `riverhog-archive-contracts`
- <a id="s-cbacdf3937"></a>`module`: `riverhog_archive_contracts`
- <a id="s-d7379f3f24"></a>`name`: `from_json_bytes`
- <a id="s-f1dd3f7107"></a>`owner`: `riverhog_archive_contracts.RecoveryDescriptor`
- <a id="s-fcade61773"></a>`unit`: `member`

### Declared structure

- <a id="s-b75afa825f"></a>`kind`: `"classmethod"`
- <a id="s-dff4d28bc0"></a>`signature`: `"\"(cls, content: 'bytes \| str') -> 'RecoveryDescriptor'\""`

## Maintained corroboration

### Related interface records

- [RecoveryDescriptor](riverhog-archive-contracts-recoverydescriptor.md)

## Governing policies

- <a id="pa-5f6aaed484"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-archive-contracts:riverhog_archive_contracts](../../../evidence/sources.md#src-4557222ddc) — `packages/riverhog-archive-contracts/src/riverhog_archive_contracts/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_archive_contracts.RecoveryDescriptor.from_json_bytes`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 5d0d30d34ad96983b8efd4fe7caf641bb244acf4f0bf34a1cd6da5f3af5bf46c -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, content: 'bytes | str') -> 'RecoveryDescriptor'\""
  },
  "distribution": "riverhog-archive-contracts",
  "module": "riverhog_archive_contracts",
  "name": "from_json_bytes",
  "owner": "riverhog_archive_contracts.RecoveryDescriptor",
  "unit": "member"
}
```

</details>
