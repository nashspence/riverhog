# riverhog_archive_contracts.CollectionArchiveManifest.from_json_bytes

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-archive-contracts:riverhog-archive-contracts-collectionarch-3268a1ca8f:0c4424c572 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-archive-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-d07670108e"></a>
- <a id="s-3b7987e4b7"></a>`distribution`: `riverhog-archive-contracts`
- <a id="s-0ed5784e1e"></a>`module`: `riverhog_archive_contracts`
- <a id="s-e0af4bf9be"></a>`name`: `from_json_bytes`
- <a id="s-a55dc9685b"></a>`owner`: `riverhog_archive_contracts.CollectionArchiveManifest`
- <a id="s-6bdbdcf339"></a>`unit`: `member`

### Declared structure

- <a id="s-e898d44299"></a>`kind`: `"classmethod"`
- <a id="s-dfb6e63b05"></a>`signature`: `"\"(cls, content: 'bytes \| str') -> 'CollectionArchiveManifest'\""`

## Maintained corroboration

### Related interface records

- [riverhog_archive_contracts.CollectionArchiveManifest](riverhog-archive-contracts-collectionarchivemanifest.md)

## Governing policies

- <a id="pa-620be2a45a"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-archive-contracts:riverhog_archive_contracts](../../../evidence/sources.md#src-4557222ddc) — `packages/riverhog-archive-contracts/src/riverhog_archive_contracts/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_archive_contracts.CollectionArchiveManifest.from_json_bytes`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: ca275c77e55a89c1b1e36722555258c6cccddeb58c6e56de9b132e1caa46b66d -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, content: 'bytes | str') -> 'CollectionArchiveManifest'\""
  },
  "distribution": "riverhog-archive-contracts",
  "module": "riverhog_archive_contracts",
  "name": "from_json_bytes",
  "owner": "riverhog_archive_contracts.CollectionArchiveManifest",
  "unit": "member"
}
```
