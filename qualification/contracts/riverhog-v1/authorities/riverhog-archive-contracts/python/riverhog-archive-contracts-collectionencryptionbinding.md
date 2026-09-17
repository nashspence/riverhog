# riverhog_archive_contracts.CollectionEncryptionBinding

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-archive-contracts:riverhog-archive-contracts-collectionencr-143bf3a40a:388f7ea78e -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-archive-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-c1a505007b"></a>
- <a id="s-daf5bf6ae1"></a>`distribution`: `riverhog-archive-contracts`
- <a id="s-4a167c1d1a"></a>`module`: `riverhog_archive_contracts`
- <a id="s-8be523ead0"></a>`name`: `CollectionEncryptionBinding`
- <a id="s-00c6c4d499"></a>`unit`: `export`

### Declared structure

- <a id="s-efec42dbb7"></a>`kind`: `"class"`
- <a id="s-d3a7efe28f"></a>`signature`: `"\"(format: 'str', passphrase_id: 'str') -> None\""`

#### Dataclass fields

| Field | Type | Default |
|---|---|---|
| <a id="s-30d8d87857"></a>`format` | `'str'` | `required` |
| <a id="s-21ebc2cddf"></a>`passphrase_id` | `'str'` | `required` |

## Governing policies

- <a id="pa-e761b07992"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-archive-contracts:riverhog_archive_contracts](../../../evidence/sources.md#src-4557222ddc) — [packages/riverhog-archive-contracts/src/riverhog\_archive\_contracts/\_\_init\_\_.py](../../../../../../packages/riverhog-archive-contracts/src/riverhog_archive_contracts/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_archive_contracts.CollectionEncryptionBinding`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 429e0345c43916c9b6fb1486e975c09dd6dc7d6ec51663a8ed23582deafb0101 -->

```json
{
  "contract": {
    "fields": [
      {
        "default": "required",
        "name": "format",
        "type": "'str'"
      },
      {
        "default": "required",
        "name": "passphrase_id",
        "type": "'str'"
      }
    ],
    "kind": "class",
    "signature": "\"(format: 'str', passphrase_id: 'str') -> None\""
  },
  "distribution": "riverhog-archive-contracts",
  "module": "riverhog_archive_contracts",
  "name": "CollectionEncryptionBinding",
  "unit": "export"
}
```

</details>
