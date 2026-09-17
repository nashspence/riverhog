# stove0_core.WorkFailure

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-workfailure:1e4fa9d338 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-1df2f7325c"></a>
- <a id="s-2e88b5d7a5"></a>`distribution`: `stove0-server`
- <a id="s-a52d535c83"></a>`module`: `stove0_core`
- <a id="s-a9ee4ec0d2"></a>`name`: `WorkFailure`
- <a id="s-5b867c9f0e"></a>`unit`: `export`

### Declared structure

- <a id="s-6b51d5584f"></a>`kind`: `"class"`
- <a id="s-937c6ab6e4"></a>`signature`: `"'(*, code: Annotated[str, MinLen(min_length=1), MaxLen(max_length=160)], message: Annotated[str, MinLen(min_length=1), MaxLen(max_length=1000)], retryable: bool) -> None'"`

#### Validated model schema

<a id="s-0796b14e91"></a>

- <a id="s-c5285e142b"></a>`type`: `"object"`
- <a id="s-578d862395"></a>`additionalProperties`: `false`
- <a id="s-92dc5cc679"></a>`required`: `["code","message","retryable"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-adb56d0389"></a>`code` | yes | type="string"; maxLength=160; minLength=1 |  |
| <a id="s-fa7d1c534e"></a>`message` | yes | type="string"; maxLength=1000; minLength=1 |  |
| <a id="s-c51f03aaba"></a>`retryable` | yes | type="boolean" |  |

## Governing policies

- <a id="pa-7c57ec297a"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-server:stove0_core](../../../evidence/sources/authorities.md#src-7558b08e7f) — [reference/stove0/application/server/src/stove0\_core/\_\_init\_\_.py](../../../../../../reference/stove0/application/server/src/stove0_core/__init__.py)

### Machine authority

- `/external_contract/python/stove0_core.WorkFailure`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: fcd8c647e73832947bdebcf6c6ee395bd044d184b7cc9859175c0421a2e55b60 -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
      "additionalProperties": false,
      "properties": {
        "code": {
          "maxLength": 160,
          "minLength": 1,
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
    "signature": "'(*, code: Annotated[str, MinLen(min_length=1), MaxLen(max_length=160)], message: Annotated[str, MinLen(min_length=1), MaxLen(max_length=1000)], retryable: bool) -> None'"
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "WorkFailure",
  "unit": "export"
}
```

</details>
