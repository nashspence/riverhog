# stove0_core.WorkInapplicable

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-workinapplicable:35a55644c3 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-e895c0cf56"></a>
- <a id="s-1932af15f3"></a>`distribution`: `stove0-server`
- <a id="s-0e6fafe095"></a>`module`: `stove0_core`
- <a id="s-7f1b51dd48"></a>`name`: `WorkInapplicable`
- <a id="s-27f12592d0"></a>`unit`: `export`

### Declared structure

- <a id="s-23d67b11c1"></a>`kind`: `"class"`
- <a id="s-b889ef54ab"></a>`signature`: `"'(*, code: Annotated[str, MinLen(min_length=1), MaxLen(max_length=160)], message: Annotated[str, MinLen(min_length=1), MaxLen(max_length=1000)]) -> None'"`

#### Validated model schema

<a id="s-ce5f60173b"></a>

- <a id="s-0382d6e91f"></a>`type`: `"object"`
- <a id="s-c1d9bc4f86"></a>`additionalProperties`: `false`
- <a id="s-ef4a9ee2a4"></a>`required`: `["code","message"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-78cb8ceb83"></a>`code` | yes | type="string"; maxLength=160; minLength=1 |  |
| <a id="s-195db13795"></a>`message` | yes | type="string"; maxLength=1000; minLength=1 |  |

## Governing policies

- <a id="pa-0646eb3115"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-server:stove0_core](../../../evidence/sources/authorities.md#src-7558b08e7f) — [reference/stove0/application/server/src/stove0\_core/\_\_init\_\_.py](../../../../../../reference/stove0/application/server/src/stove0_core/__init__.py)

### Machine authority

- `/external_contract/python/stove0_core.WorkInapplicable`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 75e95cb66eadc7a8c3f99571a6dcae0bfa7ce5ac1dba076c412d97eeddec36b1 -->

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
        }
      },
      "required": [
        "code",
        "message"
      ],
      "type": "object"
    },
    "signature": "'(*, code: Annotated[str, MinLen(min_length=1), MaxLen(max_length=160)], message: Annotated[str, MinLen(min_length=1), MaxLen(max_length=1000)]) -> None'"
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "WorkInapplicable",
  "unit": "export"
}
```

</details>
