# riverhog_provenance.FileProvenanceBinding

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-provenance:riverhog-provenance-fileprovenancebinding:deaedefd9f -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-provenance](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-0773d12145"></a>
- <a id="s-26a8126566"></a>`distribution`: `riverhog-provenance`
- <a id="s-ae10cd4e03"></a>`module`: `riverhog_provenance`
- <a id="s-a4b1168082"></a>`name`: `FileProvenanceBinding`
- <a id="s-4577a88bbf"></a>`unit`: `export`

### Declared structure

- <a id="s-0619bc1f0d"></a>`kind`: `"class"`
- <a id="s-fd74641d6d"></a>`signature`: `"'(path: \\'str\\', bytes: \\'int\\', sha256: \\'str\\', status: \"Literal[\\'captured\\', \\'omitted\\']\", journal_id: \\'str \| None\\' = None, current_state_id: \\'str \| None\\' = None, omission_reason: \\'str \| None\\' = None) -> None'"`

#### Dataclass fields

| Field | Type | Default |
|---|---|---|
| <a id="s-d9ee6148d2"></a>`path` | `'str'` | `required` |
| <a id="s-d74094fd5f"></a>`bytes` | `'int'` | `required` |
| <a id="s-c4ce8d3ebb"></a>`sha256` | `'str'` | `required` |
| <a id="s-8381149583"></a>`status` | `"Literal['captured', 'omitted']"` | `required` |
| <a id="s-4f63b48705"></a>`journal_id` | `'str \| None'` | `None` |
| <a id="s-2185563c92"></a>`current_state_id` | `'str \| None'` | `None` |
| <a id="s-adcaad18df"></a>`omission_reason` | `'str \| None'` | `None` |

## Governing policies

- <a id="pa-5838d53f17"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-provenance:riverhog_provenance](../../../evidence/sources/authorities.md#src-38ef3a6054) — [packages/riverhog-provenance/src/riverhog\_provenance/\_\_init\_\_.py](../../../../../../packages/riverhog-provenance/src/riverhog_provenance/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_provenance.FileProvenanceBinding`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 87630c2ca59d6dca73e7859a1784e9f704c8415f7781c7005b51275e3e613849 -->

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
  "name": "FileProvenanceBinding",
  "unit": "export"
}
```

</details>
