# stove0_target_protocol.TargetRuntimeAuthority

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-protocol:stove0-target-protocol-targetruntimeauthority:2d89e78dd2 -->

Exact externally visible contract owned by this semantic dossier.

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
- <a id="s-42d5bb21fe"></a>`title`: TargetRuntimeAuthority
- <a id="s-66673791c9"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-c2bfdfd403"></a>`allow_insecure_http` | no | type="boolean" |  |
| <a id="s-c652ffe607"></a>`capability_token` | yes | type="string"; minLength=1; maxLength=4096 |  |
| <a id="s-764f70019b"></a>`riverhog_base_url` | yes | type="string"; minLength=1; maxLength=2048 |  |
| <a id="s-d4a36defe1"></a>`transport` | no | type="string"; const="riverhog-capability/v1" |  |

## Governing policies

- <a id="pa-df597579f2"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-target-protocol:stove0_target_protocol](../../../evidence/sources.md#src-f4f0b22026) — `reference/stove0/packages/target-protocol/src/stove0_target_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_target_protocol.TargetRuntimeAuthority`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: b196a837c99002e5bb5651e433cb93a465eee190ae12093e604ed12d978198be -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
      "additionalProperties": false,
      "properties": {
        "allow_insecure_http": {
          "default": false,
          "title": "Allow Insecure Http",
          "type": "boolean"
        },
        "capability_token": {
          "maxLength": 4096,
          "minLength": 1,
          "title": "Capability Token",
          "type": "string"
        },
        "riverhog_base_url": {
          "maxLength": 2048,
          "minLength": 1,
          "title": "Riverhog Base Url",
          "type": "string"
        },
        "transport": {
          "const": "riverhog-capability/v1",
          "default": "riverhog-capability/v1",
          "title": "Transport",
          "type": "string"
        }
      },
      "required": [
        "riverhog_base_url",
        "capability_token"
      ],
      "title": "TargetRuntimeAuthority",
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
