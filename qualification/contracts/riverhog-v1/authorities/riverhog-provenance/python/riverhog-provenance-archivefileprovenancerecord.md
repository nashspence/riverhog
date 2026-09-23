# riverhog_provenance.ArchiveFileProvenanceRecord

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-provenance:riverhog-provenance-archivefileprovenancerecord:d04b71dfe6 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-provenance](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-6b53ac3ebc"></a>
- <a id="s-1a724de657"></a>`distribution`: `riverhog-provenance`
- <a id="s-637889f9aa"></a>`module`: `riverhog_provenance`
- <a id="s-55663d8bba"></a>`name`: `ArchiveFileProvenanceRecord`
- <a id="s-cfa91605e9"></a>`unit`: `export`

### Declared structure

- <a id="s-4caf755412"></a>`kind`: `"class"`
- <a id="s-809e2a7c2e"></a>`signature`: `"'(path: \\'str\\', bytes: \\'int\\', sha256: \\'str\\', status: \"Literal[\\'captured\\', \\'omitted\\']\", journal_id: \\'str \| None\\' = None, current_state_id: \\'str \| None\\' = None, omission_reason: \\'str \| None\\' = None) -> None'"`

#### Dataclass fields

| Field | Type | Default |
|---|---|---|
| <a id="s-8e3ce20369"></a>`path` | `'str'` | `required` |
| <a id="s-48aa041df2"></a>`bytes` | `'int'` | `required` |
| <a id="s-45a8168750"></a>`sha256` | `'str'` | `required` |
| <a id="s-e8b921ef1e"></a>`status` | `"Literal['captured', 'omitted']"` | `required` |
| <a id="s-ce08364e66"></a>`journal_id` | `'str \| None'` | `None` |
| <a id="s-9f35254f50"></a>`current_state_id` | `'str \| None'` | `None` |
| <a id="s-c95a712bb5"></a>`omission_reason` | `'str \| None'` | `None` |

## Governing policies

- <a id="pa-8f5f049337"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-provenance:riverhog_provenance](../../../evidence/sources/authorities.md#src-38ef3a6054) — [packages/riverhog-provenance/src/riverhog\_provenance/\_\_init\_\_.py](../../../../../../packages/riverhog-provenance/src/riverhog_provenance/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_provenance.ArchiveFileProvenanceRecord`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 67ab641f32013e42d2d2c5689c35cdd41683970ed6d3e318ae43b93654a9a6f9 -->

```json
{
  "contract": {
    "fields": [
      {
        "default": "required",
        "name": "path",
        "type": "'str'"
      },
      {
        "default": "required",
        "name": "bytes",
        "type": "'int'"
      },
      {
        "default": "required",
        "name": "sha256",
        "type": "'str'"
      },
      {
        "default": "required",
        "name": "status",
        "type": "\"Literal['captured', 'omitted']\""
      },
      {
        "default": "None",
        "name": "journal_id",
        "type": "'str | None'"
      },
      {
        "default": "None",
        "name": "current_state_id",
        "type": "'str | None'"
      },
      {
        "default": "None",
        "name": "omission_reason",
        "type": "'str | None'"
      }
    ],
    "kind": "class",
    "signature": "'(path: \\'str\\', bytes: \\'int\\', sha256: \\'str\\', status: \"Literal[\\'captured\\', \\'omitted\\']\", journal_id: \\'str | None\\' = None, current_state_id: \\'str | None\\' = None, omission_reason: \\'str | None\\' = None) -> None'"
  },
  "distribution": "riverhog-provenance",
  "module": "riverhog_provenance",
  "name": "ArchiveFileProvenanceRecord",
  "unit": "export"
}
```

</details>
