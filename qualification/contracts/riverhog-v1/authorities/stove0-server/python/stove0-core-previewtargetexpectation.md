# stove0_core.PreviewTargetExpectation

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-previewtargetexpectation:f827a4d8e4 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-5540c981d8"></a>
- <a id="s-f66bc4e9c2"></a>`distribution`: `stove0-server`
- <a id="s-9a24fe911e"></a>`module`: `stove0_core`
- <a id="s-0fb1f46469"></a>`name`: `PreviewTargetExpectation`
- <a id="s-f3b7ee74f8"></a>`unit`: `export`

### Declared structure

- <a id="s-220435e0d5"></a>`kind`: `"class"`
- <a id="s-b122c30443"></a>`signature`: `"\"(*, branch_id: Annotated[str, MinLen(min_length=1), MaxLen(max_length=160)], work_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], plan_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]) -> None\""`

#### Validated model schema

<a id="s-f54796f926"></a>

- <a id="s-7a176a771b"></a>`type`: `"object"`
- <a id="s-2c376bbec2"></a>`additionalProperties`: `false`
- <a id="s-36ea8913a5"></a>`required`: `["branch_id","work_id","plan_sha256"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-1ecf460c34"></a>`branch_id` | yes | type="string"; maxLength=160; minLength=1 |  |
| <a id="s-17c523724d"></a>`plan_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-497f7bb404"></a>`work_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

## Governing policies

- <a id="pa-4b65831db1"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-server:stove0_core](../../../evidence/sources/authorities.md#src-7558b08e7f) — [some-implementations/stove0/application/server/src/stove0\_core/\_\_init\_\_.py](../../../../../../some-implementations/stove0/application/server/src/stove0_core/__init__.py)

### Machine authority

- `/external_contract/python/stove0_core.PreviewTargetExpectation`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 5a45ec694281e48a53397ea725779689d3daf5530930015730ef504e67ab9fdc -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
      "additionalProperties": false,
      "properties": {
        "branch_id": {
          "maxLength": 160,
          "minLength": 1,
          "type": "string"
        },
        "plan_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "type": "string"
        },
        "work_id": {
          "pattern": "^[0-9a-f]{64}$",
          "type": "string"
        }
      },
      "required": [
        "branch_id",
        "work_id",
        "plan_sha256"
      ],
      "type": "object"
    },
    "signature": "\"(*, branch_id: Annotated[str, MinLen(min_length=1), MaxLen(max_length=160)], work_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], plan_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]) -> None\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "PreviewTargetExpectation",
  "unit": "export"
}
```

</details>
