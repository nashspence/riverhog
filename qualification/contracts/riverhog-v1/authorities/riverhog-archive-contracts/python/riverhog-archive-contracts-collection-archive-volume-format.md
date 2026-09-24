# riverhog_archive_contracts.COLLECTION_ARCHIVE_VOLUME_FORMAT

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-archive-contracts:riverhog-archive-contracts-collection-arc-3550b22050:577c6eea22 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-archive-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-0e56f30e34"></a>
- <a id="s-61cc151567"></a>`distribution`: `riverhog-archive-contracts`
- <a id="s-a0b334fa15"></a>`module`: `riverhog_archive_contracts`
- <a id="s-c1667287ce"></a>`name`: `COLLECTION_ARCHIVE_VOLUME_FORMAT`
- <a id="s-7204e2ab38"></a>`unit`: `export`

### Declared structure

- <a id="s-5cb1900122"></a>`kind`: `"constant"`
- <a id="s-3195c06079"></a>`value`: `"collection-archive-volume/v1"`

## Governing policies

- <a id="pa-ccf10b1bcd"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-archive-contracts:riverhog_archive_contracts](../../../evidence/sources/authorities.md#src-4557222ddc) — [packages/riverhog-archive-contracts/src/riverhog\_archive\_contracts/\_\_init\_\_.py](../../../../../../packages/riverhog-archive-contracts/src/riverhog_archive_contracts/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_archive_contracts.COLLECTION_ARCHIVE_VOLUME_FORMAT`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: b50b16c706bc8328b61ad5c552150a6e15ad95a3ed4ebbc16b5bc20b9f98f25f -->

```json
{
  "contract": {
    "kind": "constant",
    "value": "collection-archive-volume/v1"
  },
  "distribution": "riverhog-archive-contracts",
  "module": "riverhog_archive_contracts",
  "name": "COLLECTION_ARCHIVE_VOLUME_FORMAT",
  "unit": "export"
}
```

</details>
