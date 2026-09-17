# stove0_protocol.JoinWorkBinding

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-protocol:stove0-protocol-joinworkbinding:c40b7c7cd6 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-b854206772"></a>
- <a id="s-0691d60f59"></a>`distribution`: `stove0-protocol`
- <a id="s-a59507a0fd"></a>`module`: `stove0_protocol`
- <a id="s-df624b9773"></a>`name`: `JoinWorkBinding`
- <a id="s-38db47d563"></a>`unit`: `export`

### Declared structure

- <a id="s-0cf17f2755"></a>`kind`: `"class"`
- <a id="s-ca846eea75"></a>`signature`: `"\"(*, kind: Literal['join'] = 'join', parent_work_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], branch_set_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], members: Annotated[tuple[stove0_protocol.models.JoinWorkMemberBinding, ...], MinLen(min_length=2)]) -> None\""`

#### Validated model schema

<a id="s-3564faa66a"></a>

- <a id="s-b758093e43"></a>`type`: `"object"`
- <a id="s-b4e43e69b3"></a>`additionalProperties`: `false`
- <a id="s-976b0355c3"></a>`required`: `["parent_work_id","branch_set_sha256","members"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-e4301a2336"></a>`branch_set_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-84d3afc9f6"></a>`kind` | no | type="string"; const="join"; default="join" |  |
| <a id="s-00c0487b12"></a>`members` | yes | type="array"; items=([JoinWorkMemberBinding](#s-480f0cfabe)); minItems=2 |  |
| <a id="s-7d970213a6"></a>`parent_work_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### Definitions

- [JoinWorkMemberBinding](#s-480f0cfabe)

##### <a id="s-480f0cfabe"></a>definition `JoinWorkMemberBinding`

- <a id="s-97bbab4a0a"></a>`type`: `"object"`
- <a id="s-b2709f8a29"></a>`additionalProperties`: `false`
- <a id="s-645ae4072e"></a>`required`: `["branch_id","settlement_sha256","artifact_selection_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-f37eb16b92"></a>`artifact_selection_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-9760eb82e6"></a>`branch_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-f190cf6444"></a>`producer_settlement_sha256` | no | anyOf=[(type="string"; pattern="^[0-9a-f]{64}$"); (type="null")]; default=null |  |
| <a id="s-a0ca57e696"></a>`settlement_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

## Maintained corroboration

### Related interface records

- [canonical_members](stove0-protocol-joinworkbinding-canonical-members.md)

## Governing policies

- <a id="pa-69987bcfd6"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-protocol:stove0_protocol](../../../evidence/sources/authorities.md#src-084138045e) — [reference/stove0/packages/protocol/src/stove0\_protocol/\_\_init\_\_.py](../../../../../../reference/stove0/packages/protocol/src/stove0_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_protocol.JoinWorkBinding`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 15f2e1459b1bb11ec28649cd399395b0d56ff8901b75b6bcbecce6de51e773a6 -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
      "$defs": {
        "JoinWorkMemberBinding": {
          "additionalProperties": false,
          "properties": {
            "artifact_selection_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "branch_id": {
              "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
              "type": "string"
            },
            "producer_settlement_sha256": {
              "anyOf": [
                {
                  "pattern": "^[0-9a-f]{64}$",
                  "type": "string"
                },
                {
                  "type": "null"
                }
              ],
              "default": null
            },
            "settlement_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            }
          },
          "required": [
            "branch_id",
            "settlement_sha256",
            "artifact_selection_sha256"
          ],
          "type": "object"
        }
      },
      "additionalProperties": false,
      "properties": {
        "branch_set_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "type": "string"
        },
        "kind": {
          "const": "join",
          "default": "join",
          "type": "string"
        },
        "members": {
          "items": {
            "$ref": "#/$defs/JoinWorkMemberBinding"
          },
          "minItems": 2,
          "type": "array"
        },
        "parent_work_id": {
          "pattern": "^[0-9a-f]{64}$",
          "type": "string"
        }
      },
      "required": [
        "parent_work_id",
        "branch_set_sha256",
        "members"
      ],
      "type": "object"
    },
    "signature": "\"(*, kind: Literal['join'] = 'join', parent_work_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], branch_set_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], members: Annotated[tuple[stove0_protocol.models.JoinWorkMemberBinding, ...], MinLen(min_length=2)]) -> None\""
  },
  "distribution": "stove0-protocol",
  "module": "stove0_protocol",
  "name": "JoinWorkBinding",
  "unit": "export"
}
```

</details>
