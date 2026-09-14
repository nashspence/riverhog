# riverhog_protocol.ArtifactDisposition

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-artifactdisposition:21a4516644 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-7dcc2fcc48"></a>
- <a id="s-b24e9e81de"></a>`distribution`: `riverhog-protocol`
- <a id="s-d1317e8c96"></a>`module`: `riverhog_protocol`
- <a id="s-8e0fe797f5"></a>`name`: `ArtifactDisposition`
- <a id="s-4ee33ed4fd"></a>`unit`: `export`

### Declared structure

- <a id="s-391a96bad8"></a>`kind`: `"class"`
- <a id="s-cb47d3bccc"></a>`signature`: `"\"(input_collection_id: 'CollectionId', input_archive_root_sha256: 'str', input_path: 'str', status: 'DispositionState', code: 'str \| None' = None, message: 'str \| None' = None) -> None\""`

#### Dataclass fields

| Field | Type | Default |
|---|---|---|
| <a id="s-62e8ae5b27"></a>`input_collection_id` | `'CollectionId'` | `required` |
| <a id="s-c40425c181"></a>`input_archive_root_sha256` | `'str'` | `required` |
| <a id="s-8ba53b1087"></a>`input_path` | `'str'` | `required` |
| <a id="s-15ee184e1b"></a>`status` | `'DispositionState'` | `required` |
| <a id="s-c50fcab5ca"></a>`code` | `'str \| None'` | `None` |
| <a id="s-907fdc661c"></a>`message` | `'str \| None'` | `None` |

## Maintained corroboration

### Related interface records

- [as_dict](riverhog-protocol-artifactdisposition-as-dict.md)
- [from_mapping](riverhog-protocol-artifactdisposition-from-mapping.md)

## Governing policies

- <a id="pa-abf467171a"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources.md#src-19e35f15d9) — `packages/riverhog-protocol/src/riverhog_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_protocol.ArtifactDisposition`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: c595c56792546a487a181c34f2f2c22ab1a541595f62559fb53beababa8f53fb -->

```json
{
  "contract": {
    "fields": [
      {
        "default": "required",
        "name": "input_collection_id",
        "type": "'CollectionId'"
      },
      {
        "default": "required",
        "name": "input_archive_root_sha256",
        "type": "'str'"
      },
      {
        "default": "required",
        "name": "input_path",
        "type": "'str'"
      },
      {
        "default": "required",
        "name": "status",
        "type": "'DispositionState'"
      },
      {
        "default": "None",
        "name": "code",
        "type": "'str | None'"
      },
      {
        "default": "None",
        "name": "message",
        "type": "'str | None'"
      }
    ],
    "kind": "class",
    "signature": "\"(input_collection_id: 'CollectionId', input_archive_root_sha256: 'str', input_path: 'str', status: 'DispositionState', code: 'str | None' = None, message: 'str | None' = None) -> None\""
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "ArtifactDisposition",
  "unit": "export"
}
```
