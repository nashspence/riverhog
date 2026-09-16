# stove0_target_support.TargetRuntimeAuthority

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-support:stove0-target-support-targetruntimeauthority:3ade464029 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-caafd157a0"></a>
- <a id="s-f5661eecd8"></a>`distribution`: `stove0-target-support`
- <a id="s-260ec80b64"></a>`module`: `stove0_target_support`
- <a id="s-2c517c6f3b"></a>`name`: `TargetRuntimeAuthority`
- <a id="s-2a53a0bb3e"></a>`unit`: `export`

### Declared structure

- <a id="s-1e39d72788"></a>`kind`: `"class"`
- <a id="s-9d69363bf0"></a>`signature`: `"\"(*, transport: Literal['riverhog-capability/v1'] = 'riverhog-capability/v1', riverhog_base_url: Annotated[str, MinLen(min_length=1), MaxLen(max_length=2048)], capability_token: Annotated[str, MinLen(min_length=1), MaxLen(max_length=4096)], allow_insecure_http: bool = False) -> None\""`

#### Validated model schema

<a id="s-81ceb3a42f"></a>

- <a id="s-ba6047ccc5"></a>`type`: `"object"`
- <a id="s-2eac79e573"></a>`additionalProperties`: `false`
- <a id="s-9e04d7ac3b"></a>`required`: `["riverhog_base_url","capability_token"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-53a0f1c39f"></a>`allow_insecure_http` | no | type="boolean"; default=false |  |
| <a id="s-b980a9ecfc"></a>`capability_token` | yes | type="string"; maxLength=4096; minLength=1 |  |
| <a id="s-47781fdfae"></a>`riverhog_base_url` | yes | type="string"; maxLength=2048; minLength=1 |  |
| <a id="s-11a75faf89"></a>`transport` | no | type="string"; const="riverhog-capability/v1"; default="riverhog-capability/v1" |  |

## Governing policies

- <a id="pa-1ba35d71e2"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-target-support:stove0_target_support](../../../evidence/sources.md#src-3c01163237) — `reference/stove0/packages/target-support/src/stove0_target_support/__init__.py`

### Machine authority

- `/external_contract/python/stove0_target_support.TargetRuntimeAuthority`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: dba7506b77fe53d0963b30c246785948490a0a61168e29b3ca52c8a6af3652e3 -->

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
  "distribution": "stove0-target-support",
  "module": "stove0_target_support",
  "name": "TargetRuntimeAuthority",
  "unit": "export"
}
```

</details>
