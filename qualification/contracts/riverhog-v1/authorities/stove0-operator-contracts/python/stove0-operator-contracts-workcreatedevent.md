# stove0_operator_contracts.WorkCreatedEvent

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-operator-contracts:stove0-operator-contracts-workcreatedevent:bb663f2571 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-operator-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-3b09e77382"></a>
- <a id="s-e7ae0e2909"></a>`distribution`: `stove0-operator-contracts`
- <a id="s-6df2c46ea2"></a>`module`: `stove0_operator_contracts`
- <a id="s-dfe6c0a4e6"></a>`name`: `WorkCreatedEvent`
- <a id="s-423b9c3071"></a>`unit`: `export`

### Declared structure

- <a id="s-f6c2162c8c"></a>`kind`: `"class"`
- <a id="s-7e91262c9b"></a>`signature`: `"\"(*, id: Annotated[str, MinLen(min_length=1)], type: Literal['io.riverhog.stove0.work.created'], subject: Annotated[str, MinLen(min_length=1)], occurred_at: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=30, max_length=30, pattern='^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\\\\\\\.[0-9]{9}Z$', ascii_only=None), AfterValidator(func=<function require_canonical_utc_timestamp>)], payload: stove0_operator_contracts.WorkCreatedEventData) -> None\""`

#### Validated model schema

<a id="s-8371a97cbe"></a>

- <a id="s-c2445c347d"></a>`type`: `"object"`
- <a id="s-3474d9162d"></a>`additionalProperties`: `false`
- <a id="s-e1b5134f59"></a>`required`: `["id","type","subject","occurred_at","payload"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-2007241504"></a>`id` | yes | type="string"; minLength=1 |  |
| <a id="s-abaa04e89b"></a>`occurred_at` | yes | type="string"; maxLength=30; minLength=30; pattern="^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{9}Z$" |  |
| <a id="s-a1f7a142e2"></a>`payload` | yes | [WorkCreatedEventData](#s-6085de5cba) |  |
| <a id="s-0fa46ef090"></a>`subject` | yes | type="string"; minLength=1 |  |
| <a id="s-18839a3c6a"></a>`type` | yes | type="string"; const="io.riverhog.stove0.work.created" |  |

##### Definitions

- [WorkCreatedEventData](#s-6085de5cba)

##### <a id="s-6085de5cba"></a>definition `WorkCreatedEventData`

- <a id="s-83fc9713cc"></a>`type`: `"object"`
- <a id="s-037b2a079a"></a>`additionalProperties`: `false`
- <a id="s-ca374a66ed"></a>`required`: `["work_id","phase"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-cdec4e381b"></a>`branch_set_sha256` | no | anyOf=[(type="string"; pattern="^[0-9a-f]{64}$"); (type="null")]; default=null |  |
| <a id="s-6c55526b86"></a>`join_plan_sha256` | no | anyOf=[(type="string"; pattern="^[0-9a-f]{64}$"); (type="null")]; default=null |  |
| <a id="s-eba0cb267a"></a>`parent_work_id` | no | anyOf=[(type="string"; pattern="^[0-9a-f]{64}$"); (type="null")]; default=null |  |
| <a id="s-5ad7514f0b"></a>`phase` | yes | type="string"; enum=["eligible","claimed","observing","planning","target_preflight","queued","executing","output_finalizing","verifying","settled","source_collection_retirement_pending","coordinating","abandon_pending","complete","inapplicable","failed","canceled"] |  |
| <a id="s-2819be973f"></a>`work_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

## Maintained corroboration

### Related interface records

- [exact_subject](stove0-operator-contracts-workcreatedevent-exact-subject.md)

## Governing policies

- <a id="pa-8a2b43ce0a"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-operator-contracts:stove0_operator_contracts](../../../evidence/sources/authorities.md#src-51ad84528d) — [some-implementations/stove0/packages/operator-contracts/src/stove0\_operator\_contracts/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/operator-contracts/src/stove0_operator_contracts/__init__.py)

### Machine authority

- `/external_contract/python/stove0_operator_contracts.WorkCreatedEvent`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: b79d399608d280f01422c5e885825584b982dd0f419a39a5dfb31813b2ee3223 -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
      "$defs": {
        "WorkCreatedEventData": {
          "additionalProperties": false,
          "properties": {
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
            "join_plan_sha256": {
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
            "parent_work_id": {
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
            "phase": {
              "enum": [
                "eligible",
                "claimed",
                "observing",
                "planning",
                "target_preflight",
                "queued",
                "executing",
                "output_finalizing",
                "verifying",
                "settled",
                "source_collection_retirement_pending",
                "coordinating",
                "abandon_pending",
                "complete",
                "inapplicable",
                "failed",
                "canceled"
              ],
              "type": "string"
            },
            "work_id": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            }
          },
          "required": [
            "work_id",
            "phase"
          ],
          "type": "object"
        }
      },
      "additionalProperties": false,
      "properties": {
        "id": {
          "minLength": 1,
          "type": "string"
        },
        "occurred_at": {
          "maxLength": 30,
          "minLength": 30,
          "pattern": "^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{9}Z$",
          "type": "string"
        },
        "payload": {
          "$ref": "#/$defs/WorkCreatedEventData"
        },
        "subject": {
          "minLength": 1,
          "type": "string"
        },
        "type": {
          "const": "io.riverhog.stove0.work.created",
          "type": "string"
        }
      },
      "required": [
        "id",
        "type",
        "subject",
        "occurred_at",
        "payload"
      ],
      "type": "object"
    },
    "signature": "\"(*, id: Annotated[str, MinLen(min_length=1)], type: Literal['io.riverhog.stove0.work.created'], subject: Annotated[str, MinLen(min_length=1)], occurred_at: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=30, max_length=30, pattern='^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\\\\\\\.[0-9]{9}Z$', ascii_only=None), AfterValidator(func=<function require_canonical_utc_timestamp>)], payload: stove0_operator_contracts.WorkCreatedEventData) -> None\""
  },
  "distribution": "stove0-operator-contracts",
  "module": "stove0_operator_contracts",
  "name": "WorkCreatedEvent",
  "unit": "export"
}
```

</details>
