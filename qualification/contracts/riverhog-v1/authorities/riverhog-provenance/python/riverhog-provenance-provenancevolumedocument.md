# riverhog_provenance.ProvenanceVolumeDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-provenance:riverhog-provenance-provenancevolumedocument:c918167837 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-provenance](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-8ed9f2d910"></a>
- <a id="s-fa0def82e6"></a>`distribution`: `riverhog-provenance`
- <a id="s-f221669769"></a>`module`: `riverhog_provenance`
- <a id="s-414054d5bf"></a>`name`: `ProvenanceVolumeDocument`
- <a id="s-e164d75201"></a>`unit`: `export`

### Declared structure

- <a id="s-882817c891"></a>`kind`: `"class"`
- <a id="s-c17ff2b5ca"></a>`signature`: `"\"(archive_generation: 'str', archive_tree_sha256: 'str', sequence: 'int', payload: 'ProvenancePayloadIdentity', first_file_order: 'int \| None' = None, file_count: 'int \| None' = None, journal_id: 'str \| None' = None, journal_offset: 'int \| None' = None, journal_bytes: 'int \| None' = None, journal_sha256: 'str \| None' = None, format: 'str' = 'riverhog-provenance-volume/v1') -> None\""`

#### Dataclass fields

| Field | Type | Default |
|---|---|---|
| <a id="s-d07f219bbb"></a>`archive_generation` | `'str'` | `required` |
| <a id="s-afd6e11bae"></a>`archive_tree_sha256` | `'str'` | `required` |
| <a id="s-950e10147b"></a>`sequence` | `'int'` | `required` |
| <a id="s-6f57292056"></a>`payload` | `'ProvenancePayloadIdentity'` | `required` |
| <a id="s-663a2c7ccd"></a>`first_file_order` | `'int \| None'` | `None` |
| <a id="s-7ec6bf0516"></a>`file_count` | `'int \| None'` | `None` |
| <a id="s-33d08fac2c"></a>`journal_id` | `'str \| None'` | `None` |
| <a id="s-ed57ad7096"></a>`journal_offset` | `'int \| None'` | `None` |
| <a id="s-1eae6d8438"></a>`journal_bytes` | `'int \| None'` | `None` |
| <a id="s-2ca4805c9a"></a>`journal_sha256` | `'str \| None'` | `None` |
| <a id="s-2c2e0878b1"></a>`format` | `'str'` | `'riverhog-provenance-volume/v1'` |

## Maintained corroboration

### Related interface records

- [from_json_bytes](riverhog-provenance-provenancevolumedocument-from-json-bytes.md)
- [metadata_path](riverhog-provenance-provenancevolumedocument-metadata-path.md)
- [to_json_bytes](riverhog-provenance-provenancevolumedocument-to-json-bytes.md)
- [to_mapping](riverhog-provenance-provenancevolumedocument-to-mapping.md)

## Governing policies

- <a id="pa-65f1742c19"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-provenance:riverhog_provenance](../../../evidence/sources/authorities.md#src-38ef3a6054) — [packages/riverhog-provenance/src/riverhog\_provenance/\_\_init\_\_.py](../../../../../../packages/riverhog-provenance/src/riverhog_provenance/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_provenance.ProvenanceVolumeDocument`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: c86e31c36bb1371c0976476b178b59a860f06c469d9f2b98dbd9845bf3ee5384 -->

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
        "default": "required",
        "name": "payload",
        "type": "'ProvenancePayloadIdentity'"
      },
      {
        "default": "None",
        "name": "first_file_order",
        "type": "'int | None'"
      },
      {
        "default": "None",
        "name": "file_count",
        "type": "'int | None'"
      },
      {
        "default": "None",
        "name": "journal_id",
        "type": "'str | None'"
      },
      {
        "default": "None",
        "name": "journal_offset",
        "type": "'int | None'"
      },
      {
        "default": "None",
        "name": "journal_bytes",
        "type": "'int | None'"
      },
      {
        "default": "None",
        "name": "journal_sha256",
        "type": "'str | None'"
      },
      {
        "default": "'riverhog-provenance-volume/v1'",
        "name": "format",
        "type": "'str'"
      }
    ],
    "kind": "class",
    "signature": "\"(archive_generation: 'str', archive_tree_sha256: 'str', sequence: 'int', payload: 'ProvenancePayloadIdentity', first_file_order: 'int | None' = None, file_count: 'int | None' = None, journal_id: 'str | None' = None, journal_offset: 'int | None' = None, journal_bytes: 'int | None' = None, journal_sha256: 'str | None' = None, format: 'str' = 'riverhog-provenance-volume/v1') -> None\""
  },
  "distribution": "riverhog-provenance",
  "module": "riverhog_provenance",
  "name": "ProvenanceVolumeDocument",
  "unit": "export"
}
```

</details>
