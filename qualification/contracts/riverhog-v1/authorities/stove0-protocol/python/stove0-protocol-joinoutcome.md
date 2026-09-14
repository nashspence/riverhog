# stove0_protocol.JoinOutcome

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-protocol:stove0-protocol-joinoutcome:b3ceab1c51 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-e1bc549b7f"></a>
- <a id="s-95f27f9801"></a>`distribution`: `stove0-protocol`
- <a id="s-8326d5c807"></a>`module`: `stove0_protocol`
- <a id="s-bafcd7c452"></a>`name`: `JoinOutcome`
- <a id="s-2ed833221d"></a>`unit`: `export`

### Declared structure

- <a id="s-f7b77d1f1a"></a>`kind`: `"class"`
- <a id="s-883c64b98f"></a>`signature`: `"\"(*, format: Literal['stove0-join-outcome/v1'] = 'stove0-join-outcome/v1', work_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], workflow_plan_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], join_plan_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], state: Literal['failed', 'inapplicable', 'interrupted', 'canceled']) -> None\""`

#### Validated model schema

<a id="s-59622a48b3"></a>
- <a id="s-4ad552733b"></a>`title`: JoinOutcome
- <a id="s-109b14c389"></a>`description`: Current non-success projection for one resolved join work record.
- <a id="s-fd8fc72ccd"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-bade89ad86"></a>`format` | no | type="string"; const="stove0-join-outcome/v1" |  |
| <a id="s-5f4884d3a7"></a>`join_plan_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-3af8b3e403"></a>`state` | yes | type="string"; enum=["failed","inapplicable","interrupted","canceled"] |  |
| <a id="s-9045d5709f"></a>`work_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-05b871d9b0"></a>`workflow_plan_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

## Governing policies

- <a id="pa-5e139fc0e9"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-protocol:stove0_protocol](../../../evidence/sources.md#src-084138045e) — `reference/stove0/packages/protocol/src/stove0_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_protocol.JoinOutcome`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 58b29d0707fe00a77d841482e0934e87863b02f2dc681057d600f8ea463d0aaa -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
      "additionalProperties": false,
      "description": "Current non-success projection for one resolved join work record.",
      "properties": {
        "format": {
          "const": "stove0-join-outcome/v1",
          "default": "stove0-join-outcome/v1",
          "title": "Format",
          "type": "string"
        },
        "join_plan_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Join Plan Sha256",
          "type": "string"
        },
        "state": {
          "enum": [
            "failed",
            "inapplicable",
            "interrupted",
            "canceled"
          ],
          "title": "State",
          "type": "string"
        },
        "work_id": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Work Id",
          "type": "string"
        },
        "workflow_plan_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Workflow Plan Sha256",
          "type": "string"
        }
      },
      "required": [
        "work_id",
        "workflow_plan_sha256",
        "join_plan_sha256",
        "state"
      ],
      "title": "JoinOutcome",
      "type": "object"
    },
    "signature": "\"(*, format: Literal['stove0-join-outcome/v1'] = 'stove0-join-outcome/v1', work_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], workflow_plan_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], join_plan_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], state: Literal['failed', 'inapplicable', 'interrupted', 'canceled']) -> None\""
  },
  "distribution": "stove0-protocol",
  "module": "stove0_protocol",
  "name": "JoinOutcome",
  "unit": "export"
}
```
