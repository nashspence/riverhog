# stove0_observer_protocol.ObservationFailure

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-observer-protocol:stove0-observer-protocol-observationfailure:b00f7e3eae -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-observer-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-d70c3458b6"></a>
- <a id="s-ee568c2907"></a>`distribution`: `stove0-observer-protocol`
- <a id="s-bb4d7ca89c"></a>`module`: `stove0_observer_protocol`
- <a id="s-eadbb1d0ec"></a>`name`: `ObservationFailure`
- <a id="s-f481c14a43"></a>`unit`: `export`

### Declared structure

- <a id="s-5cb8853aec"></a>`kind`: `"class"`
- <a id="s-e6cb23ce46"></a>`signature`: `"\"(*, code: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], message: Annotated[str, MinLen(min_length=1), MaxLen(max_length=1000)], retryable: bool) -> None\""`

#### Validated model schema

<a id="s-8f096d07c3"></a>

- <a id="s-242e3dead1"></a>`type`: `"object"`
- <a id="s-b2565f7179"></a>`additionalProperties`: `false`
- <a id="s-51146e41a2"></a>`required`: `["code","message","retryable"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-6b2d6f2f1c"></a>`code` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-efd59c8da1"></a>`message` | yes | type="string"; maxLength=1000; minLength=1 |  |
| <a id="s-e638a92eb3"></a>`retryable` | yes | type="boolean" |  |

## Governing policies

- <a id="pa-94aadb3667"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-observer-protocol:stove0_observer_protocol](../../../evidence/sources/authorities.md#src-62450e0156) — [some-implementations/stove0/packages/observer-protocol/src/stove0\_observer\_protocol/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/observer-protocol/src/stove0_observer_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_observer_protocol.ObservationFailure`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: d856448b123cc1d675ec37e1b62d4deb05ad4cdc0b7ceca2d127b93d723f2856 -->

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
        },
        "retryable": {
          "type": "boolean"
        }
      },
      "required": [
        "code",
        "message",
        "retryable"
      ],
      "type": "object"
    },
    "signature": "\"(*, code: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], message: Annotated[str, MinLen(min_length=1), MaxLen(max_length=1000)], retryable: bool) -> None\""
  },
  "distribution": "stove0-observer-protocol",
  "module": "stove0_observer_protocol",
  "name": "ObservationFailure",
  "unit": "export"
}
```

</details>
