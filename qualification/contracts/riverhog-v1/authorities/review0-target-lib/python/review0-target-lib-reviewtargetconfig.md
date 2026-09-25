# review0_target_lib.ReviewTargetConfig

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:review0-target-lib:review0-target-lib-reviewtargetconfig:b4e5dca80e -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [review0-target-lib](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-a137b74726"></a>
- <a id="s-4b360112f3"></a>`distribution`: `review0-target-lib`
- <a id="s-c35e712c83"></a>`module`: `review0_target_lib`
- <a id="s-7fcfdd334d"></a>`name`: `ReviewTargetConfig`
- <a id="s-c74d9d74cd"></a>`unit`: `export`

### Declared structure

- <a id="s-7b8e08bebf"></a>`kind`: `"class"`
- <a id="s-05f5ea7b65"></a>`signature`: `"'(*, token_file: pathlib.Path, samplers: Annotated[tuple[review0_target_lib.app.SamplerConfig, ...], MinLen(min_length=1)]) -> None'"`

#### Validated model schema

<a id="s-9dc5e308ce"></a>

- <a id="s-ce4f1b405e"></a>`type`: `"object"`
- <a id="s-79d9e15e35"></a>`$id`: `"https://nashspence.github.io/riverhog/v1/config/a-review0-materializer.schema.json"`
- <a id="s-7a2920a0c2"></a>`$schema`: `"https://json-schema.org/draft/2020-12/schema"`
- <a id="s-a478bee950"></a>`additionalProperties`: `false`
- <a id="s-4df306873a"></a>`required`: `["token_file","samplers"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-df0aa70de8"></a>`samplers` | yes | type="array"; items=([SamplerConfig](#s-3b21bbffbc)); minItems=1 |  |
| <a id="s-416fbe45fa"></a>`token_file` | yes | type="string"; format="path" |  |

##### Definitions

- [SamplerConfig](#s-3b21bbffbc)

##### <a id="s-3b21bbffbc"></a>definition `SamplerConfig`

- <a id="s-00ce18b610"></a>`type`: `"object"`
- <a id="s-46ff9d982b"></a>`additionalProperties`: `false`
- <a id="s-b76e161381"></a>`required`: `["id","base_url","token_file","descriptor_sha256","image_id"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-2f9e32aafe"></a>`allow_insecure_http` | no | type="boolean"; default=false |  |
| <a id="s-d6b0cc9e8f"></a>`base_url` | yes | type="string"; maxLength=2048; minLength=1 |  |
| <a id="s-51174e3f6c"></a>`descriptor_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-da5c66ec11"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._-]{0,118}[a-z0-9])?$" |  |
| <a id="s-09a7e15a31"></a>`image_id` | yes | type="string"; pattern="^sha256:[0-9a-f]{64}$" |  |
| <a id="s-e5c3ec98d4"></a>`token_file` | yes | type="string"; format="path" |  |

## Maintained corroboration

### Related interface records

- [absolute_target_token_file](review0-target-lib-reviewtargetconfig-absolute-target-token-file.md)
- [canonical_samplers](review0-target-lib-reviewtargetconfig-canonical-samplers.md)

## Governing policies

- <a id="pa-7483154de3"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:review0-target-lib:review0_target_lib](../../../evidence/sources/authorities.md#src-665023c8f6) — [some-implementations/stove0/review0/support/src/review0\_target\_lib/\_\_init\_\_.py](../../../../../../some-implementations/stove0/review0/support/src/review0_target_lib/__init__.py)

### Machine authority

- `/external_contract/python/review0_target_lib.ReviewTargetConfig`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: a21a74b22c1fd319e9df0f31f10718da9e75e33131d753fa9289c303acdd748e -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
      "$defs": {
        "SamplerConfig": {
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
        }
      },
      "$id": "https://nashspence.github.io/riverhog/v1/config/a-review0-materializer.schema.json",
      "$schema": "https://json-schema.org/draft/2020-12/schema",
      "additionalProperties": false,
      "properties": {
        "samplers": {
          "items": {
            "$ref": "#/$defs/SamplerConfig"
          },
          "minItems": 1,
          "type": "array"
        },
        "token_file": {
          "format": "path",
          "type": "string"
        }
      },
      "required": [
        "token_file",
        "samplers"
      ],
      "type": "object"
    },
    "signature": "'(*, token_file: pathlib.Path, samplers: Annotated[tuple[review0_target_lib.app.SamplerConfig, ...], MinLen(min_length=1)]) -> None'"
  },
  "distribution": "review0-target-lib",
  "module": "review0_target_lib",
  "name": "ReviewTargetConfig",
  "unit": "export"
}
```

</details>
