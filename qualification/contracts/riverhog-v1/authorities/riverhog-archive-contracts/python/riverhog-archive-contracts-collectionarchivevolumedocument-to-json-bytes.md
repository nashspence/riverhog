# riverhog_archive_contracts.CollectionArchiveVolumeDocument.to_json_bytes

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-archive-contracts:riverhog-archive-contracts-collectionarch-1beb24743c:8cefa346fa -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-archive-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-846af98381"></a>
- <a id="s-7fb9de2041"></a>`distribution`: `riverhog-archive-contracts`
- <a id="s-afbb4b3e8f"></a>`module`: `riverhog_archive_contracts`
- <a id="s-e687d13542"></a>`name`: `to_json_bytes`
- <a id="s-83d8d53a47"></a>`owner`: `riverhog_archive_contracts.CollectionArchiveVolumeDocument`
- <a id="s-88fa58300e"></a>`unit`: `member`

### Declared structure

- <a id="s-65b2e432cf"></a>`kind`: `"method"`
- <a id="s-6cfb4e231c"></a>`signature`: `"\"(self) -> 'builtins.bytes'\""`

## Maintained corroboration

### Related interface records

- [CollectionArchiveVolumeDocument](riverhog-archive-contracts-collectionarchivevolumedocument.md)

## Governing policies

- <a id="pa-24e4117273"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-archive-contracts:riverhog_archive_contracts](../../../evidence/sources/authorities.md#src-4557222ddc) — [packages/riverhog-archive-contracts/src/riverhog\_archive\_contracts/\_\_init\_\_.py](../../../../../../packages/riverhog-archive-contracts/src/riverhog_archive_contracts/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_archive_contracts.CollectionArchiveVolumeDocument.to_json_bytes`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: dc510782dc98daf62b8fd58bdc0d0594c5dfa7d12bf72ed051b8378298128b28 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'builtins.bytes'\""
  },
  "distribution": "riverhog-archive-contracts",
  "module": "riverhog_archive_contracts",
  "name": "to_json_bytes",
  "owner": "riverhog_archive_contracts.CollectionArchiveVolumeDocument",
  "unit": "member"
}
```

</details>
