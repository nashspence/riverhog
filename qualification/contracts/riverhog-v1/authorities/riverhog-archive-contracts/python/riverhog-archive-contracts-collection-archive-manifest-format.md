# riverhog_archive_contracts.COLLECTION_ARCHIVE_MANIFEST_FORMAT

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-archive-contracts:riverhog-archive-contracts-collection-arc-8ce5fa262f:c673aefdf0 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-archive-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-9663148a50"></a>
- <a id="s-83e5a534d5"></a>`distribution`: `riverhog-archive-contracts`
- <a id="s-344d770e35"></a>`module`: `riverhog_archive_contracts`
- <a id="s-f266ddd382"></a>`name`: `COLLECTION_ARCHIVE_MANIFEST_FORMAT`
- <a id="s-c03bc160da"></a>`unit`: `export`

### Declared structure

- <a id="s-6b0501c443"></a>`kind`: `"constant"`
- <a id="s-31ae037b30"></a>`value`: `"collection-archive-manifest/v1"`

## Governing policies

- <a id="pa-67071083f1"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-archive-contracts:riverhog_archive_contracts](../../../evidence/sources/authorities.md#src-4557222ddc) — [packages/riverhog-archive-contracts/src/riverhog\_archive\_contracts/\_\_init\_\_.py](../../../../../../packages/riverhog-archive-contracts/src/riverhog_archive_contracts/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_archive_contracts.COLLECTION_ARCHIVE_MANIFEST_FORMAT`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: aee503d68d2e4fc2ce5243d1f89f1f720605621f92e5c34f396af1da5e19433e -->

```json
{
  "contract": {
    "kind": "constant",
    "value": "collection-archive-manifest/v1"
  },
  "distribution": "riverhog-archive-contracts",
  "module": "riverhog_archive_contracts",
  "name": "COLLECTION_ARCHIVE_MANIFEST_FORMAT",
  "unit": "export"
}
```

</details>
