# riverhog_archive_contracts.CollectionArchiveTerminalDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-archive-contracts:riverhog-archive-contracts-collectionarch-7cd5369bc6:7a4cc4a92b -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-archive-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-7db912252f"></a>
- <a id="s-01903aca70"></a>`distribution`: `riverhog-archive-contracts`
- <a id="s-9babce1b15"></a>`module`: `riverhog_archive_contracts`
- <a id="s-02603b4b69"></a>`name`: `CollectionArchiveTerminalDocument`
- <a id="s-058cfe15a3"></a>`unit`: `export`

### Declared structure

- <a id="s-02b399b148"></a>`kind`: `"class"`
- <a id="s-787e3a245b"></a>`signature`: `"'(archive_generation: \\'str\\', archive_tree_sha256: \\'str\\', sequence: \\'int\\', kind: \"Literal[\\'terminal\\']\" = \\'terminal\\', schema: \\'str\\' = \\'collection-archive-terminal/v1\\') -> None'"`

#### Dataclass fields

| Field | Type | Default |
|---|---|---|
| <a id="s-7e324b02a3"></a>`archive_generation` | `'str'` | `required` |
| <a id="s-f7183fbb5c"></a>`archive_tree_sha256` | `'str'` | `required` |
| <a id="s-228e783aac"></a>`sequence` | `'int'` | `required` |
| <a id="s-5da90164e4"></a>`kind` | `"Literal['terminal']"` | `'terminal'` |
| <a id="s-8cbd3a82cd"></a>`schema` | `'str'` | `'collection-archive-terminal/v1'` |

## Maintained corroboration

### Related interface records

- [to_json_bytes](riverhog-archive-contracts-collectionarchiveterminaldocument-to-json-bytes.md)
- [from_mapping](riverhog-archive-contracts-collectionarchiveterminaldocument-from-mapping.md)
- [from_json_bytes](riverhog-archive-contracts-collectionarchiveterminaldocument-from-json-bytes.md)
- [to_mapping](riverhog-archive-contracts-collectionarchiveterminaldocument-to-mapping.md)

## Governing policies

- <a id="pa-e523997d29"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-archive-contracts:riverhog_archive_contracts](../../../evidence/sources/authorities.md#src-4557222ddc) — [packages/riverhog-archive-contracts/src/riverhog\_archive\_contracts/\_\_init\_\_.py](../../../../../../packages/riverhog-archive-contracts/src/riverhog_archive_contracts/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_archive_contracts.CollectionArchiveTerminalDocument`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 6fe06988104ce4695bbb3a2ddfbf6abccef7bb65776e752b63cd84f62505ebea -->

```json
{
  "contract": {
    "fields": [
      {
        "default": "required",
        "name": "archive_generation",
        "type": "'str'"
      },
      {
        "default": "required",
        "name": "archive_tree_sha256",
        "type": "'str'"
      },
      {
        "default": "required",
        "name": "sequence",
        "type": "'int'"
      },
      {
        "default": "'terminal'",
        "name": "kind",
        "type": "\"Literal['terminal']\""
      },
      {
        "default": "'collection-archive-terminal/v1'",
        "name": "schema",
        "type": "'str'"
      }
    ],
    "kind": "class",
    "signature": "'(archive_generation: \\'str\\', archive_tree_sha256: \\'str\\', sequence: \\'int\\', kind: \"Literal[\\'terminal\\']\" = \\'terminal\\', schema: \\'str\\' = \\'collection-archive-terminal/v1\\') -> None'"
  },
  "distribution": "riverhog-archive-contracts",
  "module": "riverhog_archive_contracts",
  "name": "CollectionArchiveTerminalDocument",
  "unit": "export"
}
```

</details>
