# riverhog_archive_contracts.CollectionArchiveManifest.to_mapping

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-archive-contracts:riverhog-archive-contracts-collectionarch-4f1f26b49a:8f60083613 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-archive-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-0ac6ea45d3"></a>
- <a id="s-76aec92081"></a>`distribution`: `riverhog-archive-contracts`
- <a id="s-a3fc64fd85"></a>`module`: `riverhog_archive_contracts`
- <a id="s-4f92e58881"></a>`name`: `to_mapping`
- <a id="s-43d37883af"></a>`owner`: `riverhog_archive_contracts.CollectionArchiveManifest`
- <a id="s-78938813f0"></a>`unit`: `member`

### Declared structure

- <a id="s-29cce85479"></a>`kind`: `"method"`
- <a id="s-b0164cdc33"></a>`signature`: `"\"(self) -> 'dict[str, object]'\""`

## Maintained corroboration

### Related interface records

- [CollectionArchiveManifest](riverhog-archive-contracts-collectionarchivemanifest.md)

## Governing policies

- <a id="pa-caa733709a"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-archive-contracts:riverhog_archive_contracts](../../../evidence/sources/authorities.md#src-4557222ddc) — [packages/riverhog-archive-contracts/src/riverhog\_archive\_contracts/\_\_init\_\_.py](../../../../../../packages/riverhog-archive-contracts/src/riverhog_archive_contracts/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_archive_contracts.CollectionArchiveManifest.to_mapping`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: f0f670d6d06e3e2870c76bbaeb3c3b0754df07e86ef1d3e850e28e5e9c8bc15c -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'dict[str, object]'\""
  },
  "distribution": "riverhog-archive-contracts",
  "module": "riverhog_archive_contracts",
  "name": "to_mapping",
  "owner": "riverhog_archive_contracts.CollectionArchiveManifest",
  "unit": "member"
}
```

</details>
