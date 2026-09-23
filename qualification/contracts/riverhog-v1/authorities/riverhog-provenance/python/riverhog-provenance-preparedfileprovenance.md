# riverhog_provenance.PreparedFileProvenance

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-provenance:riverhog-provenance-preparedfileprovenance:8c38d20183 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-provenance](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-1bca796d3f"></a>
- <a id="s-9707187dda"></a>`distribution`: `riverhog-provenance`
- <a id="s-58c7caa074"></a>`module`: `riverhog_provenance`
- <a id="s-aadbcf941b"></a>`name`: `PreparedFileProvenance`
- <a id="s-f696784aa6"></a>`unit`: `export`

### Declared structure

- <a id="s-b110613148"></a>`kind`: `"class"`
- <a id="s-a60a22b7e1"></a>`signature`: `"\"(binding: 'ArchiveFileProvenanceRecord', journals: 'dict[str, bytes]', source: 'str') -> None\""`

#### Dataclass fields

| Field | Type | Default |
|---|---|---|
| <a id="s-90ecba1a23"></a>`binding` | `'ArchiveFileProvenanceRecord'` | `required` |
| <a id="s-609a727567"></a>`journals` | `'dict[str, bytes]'` | `required` |
| <a id="s-9548e30929"></a>`source` | `'str'` | `required` |

## Governing policies

- <a id="pa-26bdc95e0f"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-provenance:riverhog_provenance](../../../evidence/sources/authorities.md#src-38ef3a6054) — [packages/riverhog-provenance/src/riverhog\_provenance/\_\_init\_\_.py](../../../../../../packages/riverhog-provenance/src/riverhog_provenance/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_provenance.PreparedFileProvenance`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 34df230e6e9b6d7586dfdb7085cd1f185e85960c1c02e5439e0e61aa9c113baf -->

```json
{
  "contract": {
    "fields": [
      {
        "default": "required",
        "name": "binding",
        "type": "'ArchiveFileProvenanceRecord'"
      },
      {
        "default": "required",
        "name": "journals",
        "type": "'dict[str, bytes]'"
      },
      {
        "default": "required",
        "name": "source",
        "type": "'str'"
      }
    ],
    "kind": "class",
    "signature": "\"(binding: 'ArchiveFileProvenanceRecord', journals: 'dict[str, bytes]', source: 'str') -> None\""
  },
  "distribution": "riverhog-provenance",
  "module": "riverhog_provenance",
  "name": "PreparedFileProvenance",
  "unit": "export"
}
```

</details>
