# review0_target_lib.SamplerConfig

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:review0-target-lib:review0-target-lib-samplerconfig:aeca4f9e6b -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [review0-target-lib](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-ff4a4cd78f"></a>
- <a id="s-63b3c374e2"></a>`distribution`: `review0-target-lib`
- <a id="s-f37f4c567a"></a>`module`: `review0_target_lib`
- <a id="s-945497095e"></a>`name`: `SamplerConfig`
- <a id="s-b3281b8150"></a>`unit`: `export`

### Declared structure

- <a id="s-c258a53371"></a>`kind`: `"class"`
- <a id="s-513289ade5"></a>`signature`: `"\"(*, id: Annotated[str, _PydanticGeneralMetadata(pattern='^[a-z0-9]&#40;?:[a-z0-9._-]{0,118}[a-z0-9])?$')], base_url: Annotated[str, MinLen(min_length=1), MaxLen(max_length=2048)], token_file: pathlib.Path, allow_insecure_http: bool = False, descriptor_sha256: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')], image_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^sha256:[0-9a-f]{64}$', ascii_only=None)]) -> None\""`

#### Validated model schema

<a id="s-6aa993a1f1"></a>

- <a id="s-3acc7cf72b"></a>`type`: `"object"`
- <a id="s-71cea2ba26"></a>`additionalProperties`: `false`
- <a id="s-20c146e488"></a>`required`: `["id","base_url","token_file","descriptor_sha256","image_id"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-26d99db535"></a>`allow_insecure_http` | no | type="boolean"; default=false |  |
| <a id="s-a026c3e46a"></a>`base_url` | yes | type="string"; maxLength=2048; minLength=1 |  |
| <a id="s-0fb618fb6d"></a>`descriptor_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-9fa0d06e2d"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._-]{0,118}[a-z0-9])?$" |  |
| <a id="s-9a12260489"></a>`image_id` | yes | type="string"; pattern="^sha256:[0-9a-f]{64}$" |  |
| <a id="s-0904bcea44"></a>`token_file` | yes | type="string"; format="path" |  |

## Maintained corroboration

### Related interface records

- [absolute_token_file](review0-target-lib-samplerconfig-absolute-token-file.md)

## Governing policies

- <a id="pa-7058e3212f"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:review0-target-lib:review0_target_lib](../../../evidence/sources/authorities.md#src-665023c8f6) — [some-implementations/stove0/review0/support/src/review0\_target\_lib/\_\_init\_\_.py](../../../../../../some-implementations/stove0/review0/support/src/review0_target_lib/__init__.py)

### Machine authority

- `/external_contract/python/review0_target_lib.SamplerConfig`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: f295af485a228e0cda667952787605d9718c8544c6b7273f8a4e9872e6d53550 -->

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
        "base_url": {
          "maxLength": 2048,
          "minLength": 1,
          "type": "string"
        },
        "descriptor_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "type": "string"
        },
        "id": {
          "pattern": "^[a-z0-9]\u0028?:[a-z0-9._-]{0,118}[a-z0-9])?$",
          "type": "string"
        },
        "image_id": {
          "pattern": "^sha256:[0-9a-f]{64}$",
          "type": "string"
        },
        "token_file": {
          "format": "path",
          "type": "string"
        }
      },
      "required": [
        "id",
        "base_url",
        "token_file",
        "descriptor_sha256",
        "image_id"
      ],
      "type": "object"
    },
    "signature": "\"(*, id: Annotated[str, _PydanticGeneralMetadata(pattern='^[a-z0-9]\u0028?:[a-z0-9._-]{0,118}[a-z0-9])?$')], base_url: Annotated[str, MinLen(min_length=1), MaxLen(max_length=2048)], token_file: pathlib.Path, allow_insecure_http: bool = False, descriptor_sha256: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')], image_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^sha256:[0-9a-f]{64}$', ascii_only=None)]) -> None\""
  },
  "distribution": "review0-target-lib",
  "module": "review0_target_lib",
  "name": "SamplerConfig",
  "unit": "export"
}
```

</details>
