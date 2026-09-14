# stove0_operator_contracts.AdmissionRun

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-operator-contracts:stove0-operator-contracts-admissionrun:15d9669c48 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-operator-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-fb4a406e6b"></a>
- <a id="s-2037711a36"></a>`distribution`: `stove0-operator-contracts`
- <a id="s-55d2cf5a48"></a>`module`: `stove0_operator_contracts`
- <a id="s-a2f0c5be9f"></a>`name`: `AdmissionRun`
- <a id="s-67f208d163"></a>`unit`: `export`

### Declared structure

- <a id="s-0c6e627b36"></a>`kind`: `"class"`
- <a id="s-172451afe0"></a>`signature`: `"'(*, progressed: tuple[str, ...], failures: tuple[stove0_operator_contracts.SchedulerFailure, ...] = ()) -> None'"`

#### Validated model schema

<a id="s-4205c86be7"></a>
- <a id="s-6b7d234131"></a>`title`: AdmissionRun
- <a id="s-121f3b37ec"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-90d835b272"></a>`failures` | no | type="array"; items=(#/$defs/SchedulerFailure) |  |
| <a id="s-0a310f71eb"></a>`progressed` | yes | type="array"; items=(type="string") |  |

### Definitions

| Definition | Shape |
|---|---|
| <a id="s-109f10b819"></a>`SchedulerFailure` | type="object"; fields=`error`, `event_id`, `work_id`; additional keys=`additionalProperties`, `required` |

## Governing policies

- <a id="pa-7679e6dae4"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-operator-contracts:stove0_operator_contracts](../../../evidence/sources.md#src-51ad84528d) — `reference/stove0/packages/operator-contracts/src/stove0_operator_contracts/__init__.py`

### Machine authority

- `/external_contract/python/stove0_operator_contracts.AdmissionRun`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 16bc1b364a34e2812a4c8f8f2d71223e7cb90d5d3b9ac4a6bc2bbd9df33289ef -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
      "$defs": {
        "SchedulerFailure": {
          "additionalProperties": false,
          "properties": {
            "error": {
              "maxLength": 1000,
              "minLength": 1,
              "title": "Error",
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
              "default": null,
              "title": "Event Id"
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
              "default": null,
              "title": "Work Id"
            }
          },
          "required": [
            "error"
          ],
          "title": "SchedulerFailure",
          "type": "object"
        }
      },
      "additionalProperties": false,
      "properties": {
        "failures": {
          "default": [],
          "items": {
            "$ref": "#/$defs/SchedulerFailure"
          },
          "title": "Failures",
          "type": "array"
        },
        "progressed": {
          "items": {
            "type": "string"
          },
          "title": "Progressed",
          "type": "array"
        }
      },
      "required": [
        "progressed"
      ],
      "title": "AdmissionRun",
      "type": "object"
    },
    "signature": "'(*, progressed: tuple[str, ...], failures: tuple[stove0_operator_contracts.SchedulerFailure, ...] = ()) -> None'"
  },
  "distribution": "stove0-operator-contracts",
  "module": "stove0_operator_contracts",
  "name": "AdmissionRun",
  "unit": "export"
}
```
