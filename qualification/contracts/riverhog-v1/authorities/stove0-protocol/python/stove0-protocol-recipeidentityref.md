# stove0_protocol.RecipeIdentityRef

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-protocol:stove0-protocol-recipeidentityref:c17defa97f -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-c92f542f9f"></a>
- <a id="s-b97d7c69c3"></a>`distribution`: `stove0-protocol`
- <a id="s-523d51828b"></a>`module`: `stove0_protocol`
- <a id="s-d774675923"></a>`name`: `RecipeIdentityRef`
- <a id="s-18d2a98a19"></a>`unit`: `export`

### Declared structure

- <a id="s-3d3aca3a42"></a>`kind`: `"class"`
- <a id="s-eaa6095797"></a>`signature`: `"\"(*, id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], revision: Annotated[NonnegativeDecimal, Ge(ge=1)], sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]) -> None\""`

#### Validated model schema

<a id="s-d5d46152b0"></a>

- <a id="s-5733bc2802"></a>`type`: `"object"`
- <a id="s-628b0e461e"></a>`additionalProperties`: `false`
- <a id="s-19023f96be"></a>`required`: `["id","revision","sha256"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-56b8395d82"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-eb6e31b315"></a>`revision` | yes | [NonnegativeDecimal](#s-cb91f085b7); ge=1 |  |
| <a id="s-0fa1547510"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### Definitions

- [NonnegativeDecimal](#s-cb91f085b7)

##### <a id="s-cb91f085b7"></a>definition `NonnegativeDecimal`

- <a id="s-a05288601a"></a>`type`: `"string"`
- <a id="s-537e506571"></a>`pattern`: `"^(?:0\|[1-9][0-9]*)(?![\\s\\S])"`

## Maintained corroboration

### Related interface records

- [from_identity](stove0-protocol-recipeidentityref-from-identity.md)
- [to_identity](stove0-protocol-recipeidentityref-to-identity.md)

## Governing policies

- <a id="pa-738d0bf5f6"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-protocol:stove0_protocol](../../../evidence/sources/authorities.md#src-084138045e) — [some-implementations/stove0/packages/protocol/src/stove0\_protocol/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/protocol/src/stove0_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_protocol.RecipeIdentityRef`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: fabc1b153e52dbb6477ab7981d3b8186a192f118fbc72d78025ea802e243ccb5 -->

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
        "id": {
          "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
          "type": "string"
        },
        "revision": {
          "$ref": "#/$defs/NonnegativeDecimal",
          "ge": 1
        },
        "sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "type": "string"
        }
      },
      "required": [
        "id",
        "revision",
        "sha256"
      ],
      "type": "object"
    },
    "signature": "\"(*, id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], revision: Annotated[NonnegativeDecimal, Ge(ge=1)], sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]) -> None\""
  },
  "distribution": "stove0-protocol",
  "module": "stove0_protocol",
  "name": "RecipeIdentityRef",
  "unit": "export"
}
```

</details>
