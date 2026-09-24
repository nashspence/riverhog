# stove0_target_protocol.TargetOutputBindingSetIdentity

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-protocol:stove0-target-protocol-targetoutputbindin-358a0eb8dd:68283ecc96 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-6cd1d09725"></a>
- <a id="s-bb2688b0a3"></a>`distribution`: `stove0-target-protocol`
- <a id="s-1420fa1540"></a>`module`: `stove0_target_protocol`
- <a id="s-6d3ed8d28e"></a>`name`: `TargetOutputBindingSetIdentity`
- <a id="s-ec6b4f922e"></a>`unit`: `export`

### Declared structure

- <a id="s-76df4717bf"></a>`kind`: `"class"`
- <a id="s-aeeac4ce25"></a>`signature`: `"\"(*, artifact_count: Annotated[int, Ge(ge=1)], total_bytes: Annotated[NonnegativeDecimal, Ge(ge=0)], sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]) -> None\""`

#### Validated model schema

<a id="s-2643fa4f62"></a>

- <a id="s-2fac1d1549"></a>`type`: `"object"`
- <a id="s-7b5ce4655d"></a>`additionalProperties`: `false`
- <a id="s-169db7b1d6"></a>`required`: `["artifact_count","total_bytes","sha256"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-06360413e6"></a>`artifact_count` | yes | type="integer"; minimum=1 |  |
| <a id="s-8d5abdd79a"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-fc9b3f7cc0"></a>`total_bytes` | yes | [NonnegativeDecimal](#s-4460078f35); ge=0 |  |

##### Definitions

- [NonnegativeDecimal](#s-4460078f35)

##### <a id="s-4460078f35"></a>definition `NonnegativeDecimal`

- <a id="s-2c6971c0c9"></a>`type`: `"string"`
- <a id="s-2556811d02"></a>`pattern`: `"^(?:0\|[1-9][0-9]*)(?![\\s\\S])"`

## Governing policies

- <a id="pa-42b13c6d70"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-target-protocol:stove0_target_protocol](../../../evidence/sources/authorities.md#src-f4f0b22026) — [some-implementations/stove0/packages/target-protocol/src/stove0\_target\_protocol/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/target-protocol/src/stove0_target_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_target_protocol.TargetOutputBindingSetIdentity`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: acbe4b5f5c7216581b724d14621acc9ec7b6918560f6de4be74ae8aa2b214084 -->

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
        "artifact_count": {
          "minimum": 1,
          "type": "integer"
        },
        "sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "type": "string"
        },
        "total_bytes": {
          "$ref": "#/$defs/NonnegativeDecimal",
          "ge": 0
        }
      },
      "required": [
        "artifact_count",
        "total_bytes",
        "sha256"
      ],
      "type": "object"
    },
    "signature": "\"(*, artifact_count: Annotated[int, Ge(ge=1)], total_bytes: Annotated[NonnegativeDecimal, Ge(ge=0)], sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]) -> None\""
  },
  "distribution": "stove0-target-protocol",
  "module": "stove0_target_protocol",
  "name": "TargetOutputBindingSetIdentity",
  "unit": "export"
}
```

</details>
