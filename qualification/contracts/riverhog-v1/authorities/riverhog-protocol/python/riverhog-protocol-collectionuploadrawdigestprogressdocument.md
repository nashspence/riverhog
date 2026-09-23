# riverhog_protocol.CollectionUploadRawDigestProgressDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-collectionuploadrawdige-ed822c7f86:69ecca9f06 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-7e79d55b1f"></a>
- <a id="s-5d0b35929b"></a>`distribution`: `riverhog-protocol`
- <a id="s-5dede16988"></a>`module`: `riverhog_protocol`
- <a id="s-4cbbe4945c"></a>`name`: `CollectionUploadRawDigestProgressDocument`
- <a id="s-9d6a624bd9"></a>`unit`: `export`

### Declared structure

- <a id="s-4d557fcb28"></a>`kind`: `"class"`
- <a id="s-5370ee5641"></a>`signature`: `"'(*, path: str, accepted_parts: NonnegativeDecimal, expected_parts: Annotated[NonnegativeDecimal, Ge(ge=1)], complete: bool) -> None'"`

#### Validated model schema

<a id="s-7b6db1bda1"></a>

- <a id="s-cd26ee3d7e"></a>`type`: `"object"`
- <a id="s-743ce9e496"></a>`additionalProperties`: `false`
- <a id="s-c87d3f05bc"></a>`required`: `["path","accepted_parts","expected_parts","complete"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-4e774600c7"></a>`accepted_parts` | yes | [NonnegativeDecimal](#s-6f1effc781) |  |
| <a id="s-ca9a90315b"></a>`complete` | yes | type="boolean" |  |
| <a id="s-97f2b7dccc"></a>`expected_parts` | yes | [NonnegativeDecimal](#s-6f1effc781); ge=1 |  |
| <a id="s-462cb70bff"></a>`path` | yes | type="string" |  |

##### Definitions

- [NonnegativeDecimal](#s-6f1effc781)

##### <a id="s-6f1effc781"></a>definition `NonnegativeDecimal`

- <a id="s-79555c98cf"></a>`type`: `"string"`
- <a id="s-0312c9c5ca"></a>`pattern`: `"^(?:0\|[1-9][0-9]*)(?![\\s\\S])"`

## Maintained corroboration

### Related interface records

- [canonical_path](riverhog-protocol-collectionuploadrawdigestprogressdocument-canonical-path.md)
- [validate_completion](riverhog-protocol-collectionuploadrawdigestprogressdocument-validate-completion.md)

## Governing policies

- <a id="pa-fb918a1fae"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources/authorities.md#src-19e35f15d9) — [packages/riverhog-protocol/src/riverhog\_protocol/\_\_init\_\_.py](../../../../../../packages/riverhog-protocol/src/riverhog_protocol/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_protocol.CollectionUploadRawDigestProgressDocument`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 6f5021ecb84599e098894c529bdf7dea6c4357e8bac221c32ef6ea78c1e6723f -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
      "$defs": {
        "NonnegativeDecimal": {
          "pattern": "^(?:0|[1-9][0-9]*)(?![\\s\\S])",
          "type": "string"
        }
      },
      "additionalProperties": false,
      "properties": {
        "accepted_parts": {
          "$ref": "#/$defs/NonnegativeDecimal"
        },
        "complete": {
          "type": "boolean"
        },
        "expected_parts": {
          "$ref": "#/$defs/NonnegativeDecimal",
          "ge": 1
        },
        "path": {
          "type": "string"
        }
      },
      "required": [
        "path",
        "accepted_parts",
        "expected_parts",
        "complete"
      ],
      "type": "object"
    },
    "signature": "'(*, path: str, accepted_parts: NonnegativeDecimal, expected_parts: Annotated[NonnegativeDecimal, Ge(ge=1)], complete: bool) -> None'"
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "CollectionUploadRawDigestProgressDocument",
  "unit": "export"
}
```

</details>
