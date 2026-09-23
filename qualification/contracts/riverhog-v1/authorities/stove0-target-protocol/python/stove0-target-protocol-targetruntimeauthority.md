# stove0_target_protocol.TargetRuntimeAuthority

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-protocol:stove0-target-protocol-targetruntimeauthority:2d89e78dd2 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-c78ba96a4b"></a>
- <a id="s-55c88a69d7"></a>`distribution`: `stove0-target-protocol`
- <a id="s-0a75671fe6"></a>`module`: `stove0_target_protocol`
- <a id="s-ec73fecf7a"></a>`name`: `TargetRuntimeAuthority`
- <a id="s-69030aacfc"></a>`unit`: `export`

### Declared structure

- <a id="s-14db148cff"></a>`kind`: `"class"`
- <a id="s-d726788538"></a>`signature`: `"\"(*, transport: Literal['riverhog-capability/v1'] = 'riverhog-capability/v1', riverhog_base_url: Annotated[str, MinLen(min_length=1), MaxLen(max_length=2048)], capability_token: Annotated[str, MinLen(min_length=1), MaxLen(max_length=4096)], allow_insecure_http: bool = False) -> None\""`

#### Validated model schema

<a id="s-1f52d7e70c"></a>

- <a id="s-66673791c9"></a>`type`: `"object"`
- <a id="s-3eeff740a4"></a>`additionalProperties`: `false`
- <a id="s-022494f425"></a>`required`: `["riverhog_base_url","capability_token"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-c2bfdfd403"></a>`allow_insecure_http` | no | type="boolean"; default=false |  |
| <a id="s-c652ffe607"></a>`capability_token` | yes | type="string"; maxLength=4096; minLength=1 |  |
| <a id="s-764f70019b"></a>`riverhog_base_url` | yes | type="string"; maxLength=2048; minLength=1 |  |
| <a id="s-d4a36defe1"></a>`transport` | no | type="string"; const="riverhog-capability/v1"; default="riverhog-capability/v1" |  |

## Governing policies

- <a id="pa-df597579f2"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-target-protocol:stove0_target_protocol](../../../evidence/sources/authorities.md#src-f4f0b22026) — [some-implementations/stove0/packages/target-protocol/src/stove0\_target\_protocol/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/target-protocol/src/stove0_target_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_target_protocol.TargetRuntimeAuthority`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: ff4ca3a38e6dc6fd9dc2b1ca7c62cda33509c6096d4f1b3ef4f6856a8f6abe68 -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
      "additionalProperties": false,
      "properties": {
        "allow_insecure_http": {
          "default": false,
          "type": "boolean"
        },
        "capability_token": {
          "maxLength": 4096,
          "minLength": 1,
          "type": "string"
        },
        "riverhog_base_url": {
          "maxLength": 2048,
          "minLength": 1,
          "type": "string"
        },
        "transport": {
          "const": "riverhog-capability/v1",
          "default": "riverhog-capability/v1",
          "type": "string"
        }
      },
      "required": [
        "riverhog_base_url",
        "capability_token"
      ],
      "type": "object"
    },
    "signature": "\"(*, transport: Literal['riverhog-capability/v1'] = 'riverhog-capability/v1', riverhog_base_url: Annotated[str, MinLen(min_length=1), MaxLen(max_length=2048)], capability_token: Annotated[str, MinLen(min_length=1), MaxLen(max_length=4096)], allow_insecure_http: bool = False) -> None\""
  },
  "distribution": "stove0-target-protocol",
  "module": "stove0_target_protocol",
  "name": "TargetRuntimeAuthority",
  "unit": "export"
}
```

</details>
