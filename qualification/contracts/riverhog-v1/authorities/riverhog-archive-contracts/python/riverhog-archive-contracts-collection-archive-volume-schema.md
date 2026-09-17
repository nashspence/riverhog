# riverhog_archive_contracts.COLLECTION_ARCHIVE_VOLUME_SCHEMA

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-archive-contracts:riverhog-archive-contracts-collection-arc-eae0827cad:ac40f4f908 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-archive-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-5f83462470"></a>
- <a id="s-fb81bdc5f7"></a>`distribution`: `riverhog-archive-contracts`
- <a id="s-786182e924"></a>`module`: `riverhog_archive_contracts`
- <a id="s-4852dae34b"></a>`name`: `COLLECTION_ARCHIVE_VOLUME_SCHEMA`
- <a id="s-907c8ec22b"></a>`unit`: `export`

### Declared structure

- <a id="s-dd01c60847"></a>`kind`: `"constant"`
- <a id="s-c7921985f5"></a>`value`: `"collection-archive-volume/v1"`

## Governing policies

- <a id="pa-cdac002183"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-archive-contracts:riverhog_archive_contracts](../../../evidence/sources/authorities.md#src-4557222ddc) — [packages/riverhog-archive-contracts/src/riverhog\_archive\_contracts/\_\_init\_\_.py](../../../../../../packages/riverhog-archive-contracts/src/riverhog_archive_contracts/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_archive_contracts.COLLECTION_ARCHIVE_VOLUME_SCHEMA`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: afd0f59d555e56a169acdd6b61b3e7f700671ae5e536249624ae193446e1b8d9 -->

```json
{
  "contract": {
    "kind": "constant",
    "value": "collection-archive-volume/v1"
  },
  "distribution": "riverhog-archive-contracts",
  "module": "riverhog_archive_contracts",
  "name": "COLLECTION_ARCHIVE_VOLUME_SCHEMA",
  "unit": "export"
}
```

</details>
