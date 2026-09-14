# riverhog_protocol.CollectionUploadVolumeSummaryDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-collectionuploadvolumes-6342ad9aa4:2e2b43f163 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-350a9f3b05"></a>
- <a id="s-6cd4e3c695"></a>`distribution`: `riverhog-protocol`
- <a id="s-d5f334369b"></a>`module`: `riverhog_protocol`
- <a id="s-93a975870e"></a>`name`: `CollectionUploadVolumeSummaryDocument`
- <a id="s-797c32bc18"></a>`unit`: `export`

### Declared structure

- <a id="s-dff4d3736d"></a>`kind`: `"class"`
- <a id="s-e7769164e0"></a>`signature`: `"\"(*, volume_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^(?:pack\|segment)-[0-9a-f]{64}$', ascii_only=None)], sequence: Annotated[int, Strict(strict=True), Ge(ge=0)], kind: Literal['pack', 'segment']) -> None\""`

#### Validated model schema

<a id="s-a547b4b86b"></a>
- <a id="s-39556d1024"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-2eca180978"></a>`kind` | yes | type="string"; enum=["pack","segment"] |  |
| <a id="s-de746b69ba"></a>`sequence` | yes | type="integer"; minimum=0 |  |
| <a id="s-516fc5994f"></a>`volume_id` | yes | type="string"; pattern="^(?:pack\|segment)-[0-9a-f]{64}$" |  |

## Maintained corroboration

### Related interface records

- [riverhog_protocol.CollectionUploadVolumeSummaryDocument.validate_volume_identity](riverhog-protocol-collectionuploadvolumesummarydocument-validate-volume-identity.md)

## Governing policies

- <a id="pa-6cc1126bcd"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources.md#src-19e35f15d9) — `packages/riverhog-protocol/src/riverhog_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_protocol.CollectionUploadVolumeSummaryDocument`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: b89be7030f55536ebab8348edc08f3081b61af588bb0f23a2a054ada4bc8fd6a -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
      "additionalProperties": false,
      "properties": {
        "kind": {
          "enum": [
            "pack",
            "segment"
          ],
          "type": "string"
        },
        "sequence": {
          "minimum": 0,
          "type": "integer"
        },
        "volume_id": {
          "pattern": "^(?:pack|segment)-[0-9a-f]{64}$",
          "type": "string"
        }
      },
      "required": [
        "volume_id",
        "sequence",
        "kind"
      ],
      "type": "object"
    },
    "signature": "\"(*, volume_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^(?:pack|segment)-[0-9a-f]{64}$', ascii_only=None)], sequence: Annotated[int, Strict(strict=True), Ge(ge=0)], kind: Literal['pack', 'segment']) -> None\""
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "CollectionUploadVolumeSummaryDocument",
  "unit": "export"
}
```
