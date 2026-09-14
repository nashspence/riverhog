# stove0_operator_contracts.BranchSetAdmittedEventData

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-operator-contracts:stove0-operator-contracts-branchsetadmittedeventdata:f768fa48e4 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-operator-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-811f53197c"></a>
- <a id="s-88d0a14002"></a>`distribution`: `stove0-operator-contracts`
- <a id="s-f3cac77eb9"></a>`module`: `stove0_operator_contracts`
- <a id="s-7a4dad50a7"></a>`name`: `BranchSetAdmittedEventData`
- <a id="s-7066bfad7e"></a>`unit`: `export`

### Declared structure

- <a id="s-da783cc266"></a>`kind`: `"class"`
- <a id="s-3fcff0afc3"></a>`signature`: `"\"(*, work_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], phase: Literal['coordinating'], revision: Annotated[int, Ge(ge=2)], branch_set_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], branch_count: Annotated[int, Ge(ge=1)], admitted_work_count: Annotated[int, Ge(ge=1)]) -> None\""`

#### Validated model schema

<a id="s-3bcc965813"></a>
- <a id="s-24e69f7113"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-0dbe2713d3"></a>`admitted_work_count` | yes | type="integer"; minimum=1 |  |
| <a id="s-4b59d72431"></a>`branch_count` | yes | type="integer"; minimum=1 |  |
| <a id="s-23adcd7fe9"></a>`branch_set_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-2453f4e1e9"></a>`phase` | yes | type="string"; const="coordinating" |  |
| <a id="s-40244e82a2"></a>`revision` | yes | type="integer"; minimum=2 |  |
| <a id="s-75357cccc4"></a>`work_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

## Governing policies

- <a id="pa-72f39e283f"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-operator-contracts:stove0_operator_contracts](../../../evidence/sources.md#src-51ad84528d) — `reference/stove0/packages/operator-contracts/src/stove0_operator_contracts/__init__.py`

### Machine authority

- `/external_contract/python/stove0_operator_contracts.BranchSetAdmittedEventData`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: db03a0e8e51a47225c16cac885f3213d41bad9eb78701464aa061bcf0dc3a57b -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
      "additionalProperties": false,
      "properties": {
        "admitted_work_count": {
          "minimum": 1,
          "type": "integer"
        },
        "branch_count": {
          "minimum": 1,
          "type": "integer"
        },
        "branch_set_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "type": "string"
        },
        "phase": {
          "const": "coordinating",
          "type": "string"
        },
        "revision": {
          "minimum": 2,
          "type": "integer"
        },
        "work_id": {
          "pattern": "^[0-9a-f]{64}$",
          "type": "string"
        }
      },
      "required": [
        "work_id",
        "phase",
        "revision",
        "branch_set_sha256",
        "branch_count",
        "admitted_work_count"
      ],
      "type": "object"
    },
    "signature": "\"(*, work_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], phase: Literal['coordinating'], revision: Annotated[int, Ge(ge=2)], branch_set_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], branch_count: Annotated[int, Ge(ge=1)], admitted_work_count: Annotated[int, Ge(ge=1)]) -> None\""
  },
  "distribution": "stove0-operator-contracts",
  "module": "stove0_operator_contracts",
  "name": "BranchSetAdmittedEventData",
  "unit": "export"
}
```
