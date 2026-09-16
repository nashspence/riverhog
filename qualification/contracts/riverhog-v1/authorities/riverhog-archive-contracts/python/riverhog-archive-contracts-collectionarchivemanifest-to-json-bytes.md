# riverhog_archive_contracts.CollectionArchiveManifest.to_json_bytes

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-archive-contracts:riverhog-archive-contracts-collectionarch-eb9a39f2d9:c9d8d7e3c1 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-archive-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-42858d64ba"></a>
- <a id="s-07298caa56"></a>`distribution`: `riverhog-archive-contracts`
- <a id="s-3bc3a24d54"></a>`module`: `riverhog_archive_contracts`
- <a id="s-fc0fe71360"></a>`name`: `to_json_bytes`
- <a id="s-1bfeef92e1"></a>`owner`: `riverhog_archive_contracts.CollectionArchiveManifest`
- <a id="s-d491f7d981"></a>`unit`: `member`

### Declared structure

- <a id="s-d635102556"></a>`kind`: `"method"`
- <a id="s-b2f2abe9c8"></a>`signature`: `"\"(self) -> 'builtins.bytes'\""`

## Maintained corroboration

### Related interface records

- [CollectionArchiveManifest](riverhog-archive-contracts-collectionarchivemanifest.md)

## Governing policies

- <a id="pa-5a979fb13f"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-archive-contracts:riverhog_archive_contracts](../../../evidence/sources.md#src-4557222ddc) — `packages/riverhog-archive-contracts/src/riverhog_archive_contracts/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_archive_contracts.CollectionArchiveManifest.to_json_bytes`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: a9dacd1933da1e5d05c463d97c8b11edc4a181de71d361bad8e10afba8e601cf -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'builtins.bytes'\""
  },
  "distribution": "riverhog-archive-contracts",
  "module": "riverhog_archive_contracts",
  "name": "to_json_bytes",
  "owner": "riverhog_archive_contracts.CollectionArchiveManifest",
  "unit": "member"
}
```

</details>
