# stove0_core.PreviewAcceptance

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-previewacceptance:66d26f886a -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-be8dfb6f49"></a>
- <a id="s-0edf74d29c"></a>`distribution`: `stove0-server`
- <a id="s-b61610a0b6"></a>`module`: `stove0_core`
- <a id="s-d072a55126"></a>`name`: `PreviewAcceptance`
- <a id="s-64ded84d2d"></a>`unit`: `export`

### Declared structure

- <a id="s-e0771aec12"></a>`kind`: `"class"`
- <a id="s-a2d551b8d1"></a>`signature`: `"\"(*, preview_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], branch_set_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], target_plans: tuple[stove0_core.work_state.PreviewTargetExpectation, ...]) -> None\""`

#### Validated model schema

<a id="s-bd69fedff3"></a>

- <a id="s-848c16a168"></a>`type`: `"object"`
- <a id="s-525f24c867"></a>`additionalProperties`: `false`
- <a id="s-138ffe5831"></a>`required`: `["preview_sha256","branch_set_sha256","target_plans"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-9ba93024fb"></a>`branch_set_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-09b6390989"></a>`preview_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-98dac4a244"></a>`target_plans` | yes | type="array"; items=([PreviewTargetExpectation](#s-72283ea66c)) |  |

##### Definitions

- [PreviewTargetExpectation](#s-72283ea66c)

##### <a id="s-72283ea66c"></a>definition `PreviewTargetExpectation`

- <a id="s-cb5e23f2da"></a>`type`: `"object"`
- <a id="s-11adf7e994"></a>`additionalProperties`: `false`
- <a id="s-965f18d2e8"></a>`required`: `["branch_id","work_id","plan_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-6795c73d5f"></a>`branch_id` | yes | type="string"; maxLength=160; minLength=1 |  |
| <a id="s-a42059f60d"></a>`plan_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-8635d860ad"></a>`work_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

## Maintained corroboration

### Related interface records

- [canonical_targets](stove0-core-previewacceptance-canonical-targets.md)
- [from_preview](stove0-core-previewacceptance-from-preview.md)

## Governing policies

- <a id="pa-9059b40341"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-server:stove0_core](../../../evidence/sources.md#src-7558b08e7f) — [reference/stove0/application/server/src/stove0\_core/\_\_init\_\_.py](../../../../../../reference/stove0/application/server/src/stove0_core/__init__.py)

### Machine authority

- `/external_contract/python/stove0_core.PreviewAcceptance`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 637cc78c95c55eab91608a81b4e38fdfdfeb3f80c362fa39800c60784b684056 -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
      "$defs": {
        "PreviewTargetExpectation": {
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
        }
      },
      "additionalProperties": false,
      "properties": {
        "branch_set_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "type": "string"
        },
        "preview_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "type": "string"
        },
        "target_plans": {
          "items": {
            "$ref": "#/$defs/PreviewTargetExpectation"
          },
          "type": "array"
        }
      },
      "required": [
        "preview_sha256",
        "branch_set_sha256",
        "target_plans"
      ],
      "type": "object"
    },
    "signature": "\"(*, preview_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], branch_set_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], target_plans: tuple[stove0_core.work_state.PreviewTargetExpectation, ...]) -> None\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "PreviewAcceptance",
  "unit": "export"
}
```

</details>
