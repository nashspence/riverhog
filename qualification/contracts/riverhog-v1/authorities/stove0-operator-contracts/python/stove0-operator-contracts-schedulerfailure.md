# stove0_operator_contracts.SchedulerFailure

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-operator-contracts:stove0-operator-contracts-schedulerfailure:df978e1f52 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-operator-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-f1e38100b6"></a>
- <a id="s-c97abb90e8"></a>`distribution`: `stove0-operator-contracts`
- <a id="s-e121b2f226"></a>`module`: `stove0_operator_contracts`
- <a id="s-54c7b5cb13"></a>`name`: `SchedulerFailure`
- <a id="s-7f90a112c2"></a>`unit`: `export`

### Declared structure

- <a id="s-bab74aa569"></a>`kind`: `"class"`
- <a id="s-e083307c55"></a>`signature`: `"\"(*, event_id: str \| None = None, work_id: Optional[Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]] = None, error: Annotated[str, MinLen(min_length=1), MaxLen(max_length=1000)]) -> None\""`

#### Validated model schema

<a id="s-287de05b75"></a>

- <a id="s-90de1d8807"></a>`type`: `"object"`
- <a id="s-d464271837"></a>`additionalProperties`: `false`
- <a id="s-7ff09dcfdd"></a>`required`: `["error"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-1f6c06e3b0"></a>`error` | yes | type="string"; maxLength=1000; minLength=1 |  |
| <a id="s-c69f5696f6"></a>`event_id` | no | anyOf=(type="string") \| (type="null"); default=null |  |
| <a id="s-fbe7f96916"></a>`work_id` | no | anyOf=(type="string"; pattern="^[0-9a-f]{64}$") \| (type="null"); default=null |  |

## Maintained corroboration

### Related interface records

- [one_subject](stove0-operator-contracts-schedulerfailure-one-subject.md)

## Governing policies

- <a id="pa-122eda134a"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-operator-contracts:stove0_operator_contracts](../../../evidence/sources.md#src-51ad84528d) — `reference/stove0/packages/operator-contracts/src/stove0_operator_contracts/__init__.py`

### Machine authority

- `/external_contract/python/stove0_operator_contracts.SchedulerFailure`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 9d015af60d952241fd7286848e47c6df3f35c98da8abec5208782939e1347de4 -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
      "additionalProperties": false,
      "properties": {
        "error": {
          "maxLength": 1000,
          "minLength": 1,
          "type": "string"
        },
        "event_id": {
          "anyOf": [
            {
              "type": "string"
            },
            {
              "type": "null"
            }
          ],
          "default": null
        },
        "work_id": {
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
        "error"
      ],
      "type": "object"
    },
    "signature": "\"(*, event_id: str | None = None, work_id: Optional[Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]] = None, error: Annotated[str, MinLen(min_length=1), MaxLen(max_length=1000)]) -> None\""
  },
  "distribution": "stove0-operator-contracts",
  "module": "stove0_operator_contracts",
  "name": "SchedulerFailure",
  "unit": "export"
}
```

</details>
