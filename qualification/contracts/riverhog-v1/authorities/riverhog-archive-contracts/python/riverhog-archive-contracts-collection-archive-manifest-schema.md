# riverhog_archive_contracts.COLLECTION_ARCHIVE_MANIFEST_SCHEMA

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-archive-contracts:riverhog-archive-contracts-collection-arc-c5c45d832a:abc307a375 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-archive-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-d653b58512"></a>
- <a id="s-d0e260495e"></a>`distribution`: `riverhog-archive-contracts`
- <a id="s-5f66a48200"></a>`module`: `riverhog_archive_contracts`
- <a id="s-5a7ecf9585"></a>`name`: `COLLECTION_ARCHIVE_MANIFEST_SCHEMA`
- <a id="s-36e2b104ea"></a>`unit`: `export`

### Declared structure

- <a id="s-d83166008b"></a>`kind`: `"constant"`
- <a id="s-9eb924cc0f"></a>`value`: `"collection-archive-manifest/v1"`

## Governing policies

- <a id="pa-09ac3e94f8"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-archive-contracts:riverhog_archive_contracts](../../../evidence/sources/authorities.md#src-4557222ddc) — [packages/riverhog-archive-contracts/src/riverhog\_archive\_contracts/\_\_init\_\_.py](../../../../../../packages/riverhog-archive-contracts/src/riverhog_archive_contracts/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_archive_contracts.COLLECTION_ARCHIVE_MANIFEST_SCHEMA`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 61456d15fda62ca87564ba71ee697796f48e524908f05544d156310601d38f5f -->

```json
{
  "contract": {
    "kind": "constant",
    "value": "collection-archive-manifest/v1"
  },
  "distribution": "riverhog-archive-contracts",
  "module": "riverhog_archive_contracts",
  "name": "COLLECTION_ARCHIVE_MANIFEST_SCHEMA",
  "unit": "export"
}
```

</details>
