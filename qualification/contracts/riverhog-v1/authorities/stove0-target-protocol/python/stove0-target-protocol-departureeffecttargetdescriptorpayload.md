# stove0_target_protocol.DepartureEffectTargetDescriptorPayload

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-protocol:stove0-target-protocol-departureeffecttar-5174525f52:7c8d2de5a7 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-c3c33c312a"></a>
- <a id="s-b84f06209b"></a>`distribution`: `stove0-target-protocol`
- <a id="s-894a91364b"></a>`module`: `stove0_target_protocol`
- <a id="s-255d5befe6"></a>`name`: `DepartureEffectTargetDescriptorPayload`
- <a id="s-78cdefb050"></a>`unit`: `export`

### Declared structure

- <a id="s-5a022504ea"></a>`kind`: `"class"`
- <a id="s-9fe393221f"></a>`signature`: `"\"(*, format: Literal['stove0-departure-effect-target/v1'] = 'stove0-departure-effect-target/v1', implementation_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], implementation_version: Annotated[str, MinLen(min_length=1), MaxLen(max_length=120)], source_revision: Annotated[str, MinLen(min_length=1), MaxLen(max_length=200)], image_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^sha256:[0-9a-f]{64}$', ascii_only=None)], scope_identity: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]) -> None\""`

#### Validated model schema

<a id="s-5cebc5fc23"></a>

- <a id="s-99cbfbde84"></a>`type`: `"object"`
- <a id="s-0e467aa73a"></a>`additionalProperties`: `false`
- <a id="s-39fa20d6e9"></a>`required`: `["implementation_id","implementation_version","source_revision","image_id","scope_identity"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-1c2b82a121"></a>`format` | no | type="string"; const="stove0-departure-effect-target/v1"; default="stove0-departure-effect-target/v1" |  |
| <a id="s-ca38c87a56"></a>`image_id` | yes | type="string"; pattern="^sha256:[0-9a-f]{64}$" |  |
| <a id="s-739b3ccbad"></a>`implementation_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-9660215cdb"></a>`implementation_version` | yes | type="string"; maxLength=120; minLength=1 |  |
| <a id="s-d0a2945bb1"></a>`scope_identity` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-a6a420a504"></a>`source_revision` | yes | type="string"; maxLength=200; minLength=1 |  |

## Governing policies

- <a id="pa-60497276bc"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-target-protocol:stove0_target_protocol](../../../evidence/sources/authorities.md#src-f4f0b22026) — [some-implementations/stove0/packages/target-protocol/src/stove0\_target\_protocol/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/target-protocol/src/stove0_target_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_target_protocol.DepartureEffectTargetDescriptorPayload`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: ef6ceb9ee35f235942a45fd32f690d76ce2c54530d87ae5e5872c359dccaffeb -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
      "additionalProperties": false,
      "properties": {
        "format": {
          "const": "stove0-departure-effect-target/v1",
          "default": "stove0-departure-effect-target/v1",
          "type": "string"
        },
        "image_id": {
          "pattern": "^sha256:[0-9a-f]{64}$",
          "type": "string"
        },
        "implementation_id": {
          "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
          "type": "string"
        },
        "implementation_version": {
          "maxLength": 120,
          "minLength": 1,
          "type": "string"
        },
        "scope_identity": {
          "pattern": "^[0-9a-f]{64}$",
          "type": "string"
        },
        "source_revision": {
          "maxLength": 200,
          "minLength": 1,
          "type": "string"
        }
      },
      "required": [
        "implementation_id",
        "implementation_version",
        "source_revision",
        "image_id",
        "scope_identity"
      ],
      "type": "object"
    },
    "signature": "\"(*, format: Literal['stove0-departure-effect-target/v1'] = 'stove0-departure-effect-target/v1', implementation_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], implementation_version: Annotated[str, MinLen(min_length=1), MaxLen(max_length=120)], source_revision: Annotated[str, MinLen(min_length=1), MaxLen(max_length=200)], image_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^sha256:[0-9a-f]{64}$', ascii_only=None)], scope_identity: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]) -> None\""
  },
  "distribution": "stove0-target-protocol",
  "module": "stove0_target_protocol",
  "name": "DepartureEffectTargetDescriptorPayload",
  "unit": "export"
}
```

</details>
