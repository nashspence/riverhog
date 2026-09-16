# stove0_protocol.BranchOutcome

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-protocol:stove0-protocol-branchoutcome:204ae48507 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-ed0e6bd90e"></a>
- <a id="s-fd41d0605d"></a>`distribution`: `stove0-protocol`
- <a id="s-83d48ae6c4"></a>`module`: `stove0_protocol`
- <a id="s-ec5a5e650d"></a>`name`: `BranchOutcome`
- <a id="s-5ad96785e6"></a>`unit`: `export`

### Declared structure

- <a id="s-0b58debbf5"></a>`kind`: `"class"`
- <a id="s-5ff05f457f"></a>`signature`: `"\"(*, format: Literal['stove0-branch-outcome/v1'] = 'stove0-branch-outcome/v1', branch_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], work_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], workflow_plan_sha256: Optional[Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]] = None, branch_set_sha256: Optional[Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]] = None, state: Literal['failed', 'inapplicable', 'interrupted', 'canceled']) -> None\""`

#### Validated model schema

<a id="s-a247abab03"></a>

- <a id="s-53c1393412"></a>`type`: `"object"`
- <a id="s-c7cd530751"></a>`additionalProperties`: `false`
- <a id="s-b295ae5692"></a>`required`: `["branch_id","work_id","state"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-01bd34421c"></a>`branch_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-98f8aff547"></a>`branch_set_sha256` | no | anyOf=(type="string"; pattern="^[0-9a-f]{64}$") \| (type="null"); default=null |  |
| <a id="s-5b4f066dc6"></a>`format` | no | type="string"; const="stove0-branch-outcome/v1"; default="stove0-branch-outcome/v1" |  |
| <a id="s-9d9231ca67"></a>`state` | yes | type="string"; enum=["failed","inapplicable","interrupted","canceled"] |  |
| <a id="s-f27ad0208a"></a>`work_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-cb21b5c0fb"></a>`workflow_plan_sha256` | no | anyOf=(type="string"; pattern="^[0-9a-f]{64}$") \| (type="null"); default=null |  |

## Maintained corroboration

### Related interface records

- [exact_declared_plan](stove0-protocol-branchoutcome-exact-declared-plan.md)

## Governing policies

- <a id="pa-ac99b95459"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-protocol:stove0_protocol](../../../evidence/sources.md#src-084138045e) — `reference/stove0/packages/protocol/src/stove0_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_protocol.BranchOutcome`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 4ec2a1d1a8aab9994ad1e04ae1f0adf3e36f0c8a4f9a6c2d5ab862745f7212c7 -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
      "additionalProperties": false,
      "properties": {
        "branch_id": {
          "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
          "type": "string"
        },
        "branch_set_sha256": {
          "anyOf": [
            {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            {
              "type": "null"
            }
          ],
          "default": null
        },
        "format": {
          "const": "stove0-branch-outcome/v1",
          "default": "stove0-branch-outcome/v1",
          "type": "string"
        },
        "state": {
          "enum": [
            "failed",
            "inapplicable",
            "interrupted",
            "canceled"
          ],
          "type": "string"
        },
        "work_id": {
          "pattern": "^[0-9a-f]{64}$",
          "type": "string"
        },
        "workflow_plan_sha256": {
          "anyOf": [
            {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            {
              "type": "null"
            }
          ],
          "default": null
        }
      },
      "required": [
        "branch_id",
        "work_id",
        "state"
      ],
      "type": "object"
    },
    "signature": "\"(*, format: Literal['stove0-branch-outcome/v1'] = 'stove0-branch-outcome/v1', branch_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], work_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], workflow_plan_sha256: Optional[Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]] = None, branch_set_sha256: Optional[Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]] = None, state: Literal['failed', 'inapplicable', 'interrupted', 'canceled']) -> None\""
  },
  "distribution": "stove0-protocol",
  "module": "stove0_protocol",
  "name": "BranchOutcome",
  "unit": "export"
}
```

</details>
