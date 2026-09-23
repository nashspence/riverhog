# stove0_core.ClaimBinding

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-claimbinding:82a31f5db2 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-b809db080b"></a>
- <a id="s-94c1857fc6"></a>`distribution`: `stove0-server`
- <a id="s-6c8880c007"></a>`module`: `stove0_core`
- <a id="s-7936ede901"></a>`name`: `ClaimBinding`
- <a id="s-222ae778d6"></a>`unit`: `export`

### Declared structure

- <a id="s-3d22409ed8"></a>`kind`: `"class"`
- <a id="s-d9eb2f22ba"></a>`signature`: `"'(*, claim_id: Annotated[str, MinLen(min_length=1), MaxLen(max_length=160)], fence: Annotated[int, Ge(ge=1)]) -> None'"`

#### Validated model schema

<a id="s-577bfcf6b0"></a>

- <a id="s-5177d009da"></a>`type`: `"object"`
- <a id="s-82f0ec912c"></a>`additionalProperties`: `false`
- <a id="s-7084106f97"></a>`required`: `["claim_id","fence"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-897c4d7dda"></a>`claim_id` | yes | type="string"; maxLength=160; minLength=1 |  |
| <a id="s-e99e03e44d"></a>`fence` | yes | type="integer"; minimum=1 |  |

## Governing policies

- <a id="pa-de4fba4083"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-server:stove0_core](../../../evidence/sources/authorities.md#src-7558b08e7f) — [some-implementations/stove0/application/server/src/stove0\_core/\_\_init\_\_.py](../../../../../../some-implementations/stove0/application/server/src/stove0_core/__init__.py)

### Machine authority

- `/external_contract/python/stove0_core.ClaimBinding`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: e83032fcebf4494d10ec2060f3a5ac13a4345c30339ddc3f6211179eb5a41b8c -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
      "additionalProperties": false,
      "properties": {
        "claim_id": {
          "maxLength": 160,
          "minLength": 1,
          "type": "string"
        },
        "fence": {
          "minimum": 1,
          "type": "integer"
        }
      },
      "required": [
        "claim_id",
        "fence"
      ],
      "type": "object"
    },
    "signature": "'(*, claim_id: Annotated[str, MinLen(min_length=1), MaxLen(max_length=160)], fence: Annotated[int, Ge(ge=1)]) -> None'"
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "ClaimBinding",
  "unit": "export"
}
```

</details>
