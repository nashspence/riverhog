# stove0_target_protocol.TargetInapplicable

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-protocol:stove0-target-protocol-targetinapplicable:6ad25a6169 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-255af30b2b"></a>
- <a id="s-32a2c31223"></a>`distribution`: `stove0-target-protocol`
- <a id="s-92b9f0609e"></a>`module`: `stove0_target_protocol`
- <a id="s-cb1948dcd0"></a>`name`: `TargetInapplicable`
- <a id="s-a006e32614"></a>`unit`: `export`

### Declared structure

- <a id="s-2a710a2af3"></a>`kind`: `"class"`
- <a id="s-c4dc403505"></a>`signature`: `"\"(*, code: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], message: Annotated[str, MinLen(min_length=1), MaxLen(max_length=1000)]) -> None\""`

#### Validated model schema

<a id="s-607b89b2a0"></a>
- <a id="s-ffa69fa9fd"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-873c9abaec"></a>`code` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-625057ee90"></a>`message` | yes | type="string"; minLength=1; maxLength=1000 |  |

## Governing policies

- <a id="pa-847f385c92"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-target-protocol:stove0_target_protocol](../../../evidence/sources.md#src-f4f0b22026) — `reference/stove0/packages/target-protocol/src/stove0_target_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_target_protocol.TargetInapplicable`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: fa87f8d162ce1aaf7d54ab147f14cdfdc56639122972342fc74f6dc24a010ca7 -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
      "additionalProperties": false,
      "properties": {
        "code": {
          "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
          "type": "string"
        },
        "message": {
          "maxLength": 1000,
          "minLength": 1,
          "type": "string"
        }
      },
      "required": [
        "code",
        "message"
      ],
      "type": "object"
    },
    "signature": "\"(*, code: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], message: Annotated[str, MinLen(min_length=1), MaxLen(max_length=1000)]) -> None\""
  },
  "distribution": "stove0-target-protocol",
  "module": "stove0_target_protocol",
  "name": "TargetInapplicable",
  "unit": "export"
}
```
