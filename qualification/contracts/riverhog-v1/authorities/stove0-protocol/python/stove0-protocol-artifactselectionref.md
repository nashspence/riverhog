# stove0_protocol.ArtifactSelectionRef

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-protocol:stove0-protocol-artifactselectionref:5c6b257eba -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-49de63b382"></a>
- <a id="s-253aae5a09"></a>`distribution`: `stove0-protocol`
- <a id="s-62f6917187"></a>`module`: `stove0_protocol`
- <a id="s-e22a721a43"></a>`name`: `ArtifactSelectionRef`
- <a id="s-b8d88cd133"></a>`unit`: `export`

### Declared structure

- <a id="s-c64b79336f"></a>`kind`: `"class"`
- <a id="s-44d973b82a"></a>`signature`: `"\"(*, selection_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], artifact_count: Annotated[int, Ge(ge=1)], total_bytes: Annotated[int, Ge(ge=0)]) -> None\""`

#### Validated model schema

<a id="s-b010b6408b"></a>
- <a id="s-7e9f4d8e66"></a>`title`: ArtifactSelectionRef
- <a id="s-e0ea483e48"></a>`description`: Closed reference to a separately retained selection document.
- <a id="s-bfa6214ab2"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-20a6419614"></a>`artifact_count` | yes | type="integer"; minimum=1 |  |
| <a id="s-4521979d85"></a>`selection_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-05cba6e4d0"></a>`total_bytes` | yes | type="integer"; minimum=0 |  |

## Maintained corroboration

### Related interface records

- [stove0_protocol.ArtifactSelectionRef.from_selection](stove0-protocol-artifactselectionref-from-selection.md)

## Governing policies

- <a id="pa-ef659a0767"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-protocol:stove0_protocol](../../../evidence/sources.md#src-084138045e) — `reference/stove0/packages/protocol/src/stove0_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_protocol.ArtifactSelectionRef`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: c5b1dbb798889d147d61975c6a792b123257a78749fec7a69d42ec6ca873247a -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
      "additionalProperties": false,
      "description": "Closed reference to a separately retained selection document.",
      "properties": {
        "artifact_count": {
          "minimum": 1,
          "title": "Artifact Count",
          "type": "integer"
        },
        "selection_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Selection Sha256",
          "type": "string"
        },
        "total_bytes": {
          "minimum": 0,
          "title": "Total Bytes",
          "type": "integer"
        }
      },
      "required": [
        "selection_sha256",
        "artifact_count",
        "total_bytes"
      ],
      "title": "ArtifactSelectionRef",
      "type": "object"
    },
    "signature": "\"(*, selection_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], artifact_count: Annotated[int, Ge(ge=1)], total_bytes: Annotated[int, Ge(ge=0)]) -> None\""
  },
  "distribution": "stove0-protocol",
  "module": "stove0_protocol",
  "name": "ArtifactSelectionRef",
  "unit": "export"
}
```
