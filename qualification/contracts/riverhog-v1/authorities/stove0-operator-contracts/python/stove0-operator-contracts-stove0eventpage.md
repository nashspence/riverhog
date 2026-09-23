# stove0_operator_contracts.Stove0EventPage

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-operator-contracts:stove0-operator-contracts-stove0eventpage:953f89e83c -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-operator-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-6cf1c87427"></a>
- <a id="s-fd58b575c8"></a>`distribution`: `stove0-operator-contracts`
- <a id="s-f3a6e143ee"></a>`module`: `stove0_operator_contracts`
- <a id="s-e8aa9fc6f7"></a>`name`: `Stove0EventPage`
- <a id="s-5c6a732e20"></a>`unit`: `export`

### Declared structure

- <a id="s-421ddbdef2"></a>`kind`: `"class"`
- <a id="s-6b2a819afe"></a>`signature`: `"'(*, events: list[Stove0LifecycleEvent], next_cursor: str, has_more: bool) -> None'"`

#### Validated model schema

<a id="s-2ab0e0d052"></a>

- <a id="s-2f2701364f"></a>`type`: `"object"`
- <a id="s-163d724f5c"></a>`additionalProperties`: `false`
- <a id="s-06fb79401d"></a>`required`: `["events","next_cursor","has_more"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-cf3ed5a20a"></a>`events` | yes | type="array"; items=([Stove0LifecycleEvent](#s-65476282a3)) |  |
| <a id="s-f66cbd171c"></a>`has_more` | yes | type="boolean" |  |
| <a id="s-8198408c2b"></a>`next_cursor` | yes | type="string" |  |

##### Definitions

- [BranchSetAdmittedEvent](#s-696ca6dc56)
- [BranchSetAdmittedEventData](#s-e062cfc1fb)
- [EvaluationCreatedEvent](#s-154fa9e25a)
- [EvaluationCreatedEventData](#s-76b044aa8b)
- [EvaluationUpdatedEvent](#s-839e04dd67)
- [EvaluationUpdatedEventData](#s-bf4fa5097c)
- [JoinAdmittedEvent](#s-ddb7cc84c4)
- [JoinAdmittedEventData](#s-4d75b42669)
- [Stove0LifecycleEvent](#s-65476282a3)
- [WorkCreatedEvent](#s-5eaf67dd9d)
- [WorkCreatedEventData](#s-e484369c30)
- [WorkUpdatedEvent](#s-dfaa67187b)
- [WorkUpdatedEventData](#s-d34baf0de6)

##### <a id="s-696ca6dc56"></a>definition `BranchSetAdmittedEvent`

- <a id="s-dc02e1fc40"></a>`type`: `"object"`
- <a id="s-fa4d313fb2"></a>`additionalProperties`: `false`
- <a id="s-c895d13269"></a>`required`: `["id","source","type","subject","time","data"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-d2d86d3c96"></a>`data` | yes | [BranchSetAdmittedEventData](#s-e062cfc1fb) |  |
| <a id="s-088041c7e8"></a>`datacontenttype` | no | type="string"; const="application/json"; default="application/json" |  |
| <a id="s-ad76ca3ec6"></a>`id` | yes | type="string"; minLength=1 |  |
| <a id="s-811bc19c53"></a>`source` | yes | type="string"; const="urn:riverhog:stove0" |  |
| <a id="s-d51d3dadab"></a>`specversion` | no | type="string"; const="1.0"; default="1.0" |  |
| <a id="s-422c8fc3cf"></a>`subject` | yes | type="string"; minLength=1 |  |
| <a id="s-c446dc142c"></a>`time` | yes | type="string" |  |
| <a id="s-67943fadab"></a>`type` | yes | type="string"; const="io.riverhog.stove0.branch-set.admitted" |  |

##### <a id="s-e062cfc1fb"></a>definition `BranchSetAdmittedEventData`

- <a id="s-69a66fb8b8"></a>`type`: `"object"`
- <a id="s-e887e8c370"></a>`additionalProperties`: `false`
- <a id="s-54305d04f0"></a>`required`: `["work_id","phase","revision","branch_set_sha256","branch_count","admitted_work_count"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-d4c3957ee0"></a>`admitted_work_count` | yes | type="integer"; minimum=1 |  |
| <a id="s-f07db4cb3c"></a>`branch_count` | yes | type="integer"; minimum=1 |  |
| <a id="s-7fc6f39bd1"></a>`branch_set_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-9edb26ce9f"></a>`phase` | yes | type="string"; const="coordinating" |  |
| <a id="s-9e1aae83fd"></a>`revision` | yes | type="integer"; minimum=2 |  |
| <a id="s-85051b7a47"></a>`work_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-154fa9e25a"></a>definition `EvaluationCreatedEvent`

- <a id="s-8589cd42c9"></a>`type`: `"object"`
- <a id="s-1eea6940c2"></a>`additionalProperties`: `false`
- <a id="s-c10a737250"></a>`required`: `["id","source","type","subject","time","data"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-00061afc5a"></a>`data` | yes | [EvaluationCreatedEventData](#s-76b044aa8b) |  |
| <a id="s-a7dd621055"></a>`datacontenttype` | no | type="string"; const="application/json"; default="application/json" |  |
| <a id="s-89615ffd43"></a>`id` | yes | type="string"; minLength=1 |  |
| <a id="s-c60e55df8b"></a>`source` | yes | type="string"; const="urn:riverhog:stove0" |  |
| <a id="s-9b28726b6a"></a>`specversion` | no | type="string"; const="1.0"; default="1.0" |  |
| <a id="s-1f35f82699"></a>`subject` | yes | type="string"; minLength=1 |  |
| <a id="s-5540310b99"></a>`time` | yes | type="string" |  |
| <a id="s-65e1e4ff20"></a>`type` | yes | type="string"; const="io.riverhog.stove0.evaluation.created" |  |

##### <a id="s-76b044aa8b"></a>definition `EvaluationCreatedEventData`

- <a id="s-d656cf2d63"></a>`type`: `"object"`
- <a id="s-5c48dd5c91"></a>`additionalProperties`: `false`
- <a id="s-0e158e32cb"></a>`required`: `["evaluation_id","phase"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-67d43d07c1"></a>`evaluation_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-d0487605ca"></a>`phase` | yes | type="string"; enum=["planning","running","partially_complete","complete","failed","canceled"] |  |

##### <a id="s-839e04dd67"></a>definition `EvaluationUpdatedEvent`

- <a id="s-7289397dbb"></a>`type`: `"object"`
- <a id="s-5df008de17"></a>`additionalProperties`: `false`
- <a id="s-53b3025c07"></a>`required`: `["id","source","type","subject","time","data"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-e2746ec7d5"></a>`data` | yes | [EvaluationUpdatedEventData](#s-bf4fa5097c) |  |
| <a id="s-f3eb1d7d58"></a>`datacontenttype` | no | type="string"; const="application/json"; default="application/json" |  |
| <a id="s-0d186d9902"></a>`id` | yes | type="string"; minLength=1 |  |
| <a id="s-f6d946f88c"></a>`source` | yes | type="string"; const="urn:riverhog:stove0" |  |
| <a id="s-9391aa9f3b"></a>`specversion` | no | type="string"; const="1.0"; default="1.0" |  |
| <a id="s-6095eff361"></a>`subject` | yes | type="string"; minLength=1 |  |
| <a id="s-053e48c4e5"></a>`time` | yes | type="string" |  |
| <a id="s-cdf88f034c"></a>`type` | yes | type="string"; const="io.riverhog.stove0.evaluation.updated" |  |

##### <a id="s-bf4fa5097c"></a>definition `EvaluationUpdatedEventData`

- <a id="s-c77cd21391"></a>`type`: `"object"`
- <a id="s-bf86f42f3d"></a>`additionalProperties`: `false`
- <a id="s-4edc351ab6"></a>`required`: `["evaluation_id","phase","revision"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-595b155e55"></a>`evaluation_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-38ddd393db"></a>`phase` | yes | type="string"; enum=["planning","running","partially_complete","complete","failed","canceled"] |  |
| <a id="s-3563306314"></a>`revision` | yes | type="integer"; minimum=2 |  |

##### <a id="s-ddb7cc84c4"></a>definition `JoinAdmittedEvent`

- <a id="s-b38b510778"></a>`type`: `"object"`
- <a id="s-e6e0032d99"></a>`additionalProperties`: `false`
- <a id="s-c35842a067"></a>`required`: `["id","source","type","subject","time","data"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-7d98074a1f"></a>`data` | yes | [JoinAdmittedEventData](#s-4d75b42669) |  |
| <a id="s-3050f3af96"></a>`datacontenttype` | no | type="string"; const="application/json"; default="application/json" |  |
| <a id="s-815b0afa17"></a>`id` | yes | type="string"; minLength=1 |  |
| <a id="s-afd9d3befa"></a>`source` | yes | type="string"; const="urn:riverhog:stove0" |  |
| <a id="s-e03bdc1770"></a>`specversion` | no | type="string"; const="1.0"; default="1.0" |  |
| <a id="s-63c8f8a47a"></a>`subject` | yes | type="string"; minLength=1 |  |
| <a id="s-68f32492f8"></a>`time` | yes | type="string" |  |
| <a id="s-8d3583f6b2"></a>`type` | yes | type="string"; const="io.riverhog.stove0.join.admitted" |  |

##### <a id="s-4d75b42669"></a>definition `JoinAdmittedEventData`

- <a id="s-5ac46295c5"></a>`type`: `"object"`
- <a id="s-0abadf36ca"></a>`additionalProperties`: `false`
- <a id="s-48145ae281"></a>`required`: `["work_id","phase","revision","branch_set_sha256","join_plan_sha256","join_work_id"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-9070ccf71b"></a>`branch_set_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-17677bdd75"></a>`join_plan_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-2b9c6fdf28"></a>`join_work_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-da0235c651"></a>`phase` | yes | type="string"; const="coordinating" |  |
| <a id="s-132713351f"></a>`revision` | yes | type="integer"; minimum=2 |  |
| <a id="s-8feaf556f1"></a>`work_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-65476282a3"></a>definition `Stove0LifecycleEvent`

- <a id="s-5b5b90a3a2"></a>`discriminator`: `{"mapping":{"io.riverhog.stove0.branch-set.admitted":"#/$defs/BranchSetAdmittedEvent","io.riverhog.stove0.evaluation.created":"#/$defs/EvaluationCreatedEvent","io.riverhog.stove0.evaluation.updated":"#/$defs/EvaluationUpdatedEvent","io.riverhog.stove0.join.admitted":"#/$defs/JoinAdmittedEvent","io.riverhog.stove0.work.created":"#/$defs/WorkCreatedEvent","io.riverhog.stove0.work.updated":"#/$defs/WorkUpdatedEvent"},"propertyName":"type"}`

###### Exactly one must match (`oneOf`)

| Alternative | Schema |
|---|---|
| <a id="s-001f7f99c5"></a>1 | [WorkCreatedEvent](#s-5eaf67dd9d) |
| <a id="s-ba71bd07a1"></a>2 | [WorkUpdatedEvent](#s-dfaa67187b) |
| <a id="s-769511558d"></a>3 | [BranchSetAdmittedEvent](#s-696ca6dc56) |
| <a id="s-999c7d8cdc"></a>4 | [JoinAdmittedEvent](#s-ddb7cc84c4) |
| <a id="s-dd1721055a"></a>5 | [EvaluationCreatedEvent](#s-154fa9e25a) |
| <a id="s-ee0f5b3ef4"></a>6 | [EvaluationUpdatedEvent](#s-839e04dd67) |

##### <a id="s-5eaf67dd9d"></a>definition `WorkCreatedEvent`

- <a id="s-a105573d2f"></a>`type`: `"object"`
- <a id="s-c8cb7904a6"></a>`additionalProperties`: `false`
- <a id="s-262a38b220"></a>`required`: `["id","source","type","subject","time","data"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-538ebb7c26"></a>`data` | yes | [WorkCreatedEventData](#s-e484369c30) |  |
| <a id="s-cb2f03012c"></a>`datacontenttype` | no | type="string"; const="application/json"; default="application/json" |  |
| <a id="s-4882c53ca7"></a>`id` | yes | type="string"; minLength=1 |  |
| <a id="s-80a7824a80"></a>`source` | yes | type="string"; const="urn:riverhog:stove0" |  |
| <a id="s-c1750c40bb"></a>`specversion` | no | type="string"; const="1.0"; default="1.0" |  |
| <a id="s-80fe67098e"></a>`subject` | yes | type="string"; minLength=1 |  |
| <a id="s-b6397c8ca9"></a>`time` | yes | type="string" |  |
| <a id="s-0913ecfaa0"></a>`type` | yes | type="string"; const="io.riverhog.stove0.work.created" |  |

##### <a id="s-e484369c30"></a>definition `WorkCreatedEventData`

- <a id="s-ce14a3add1"></a>`type`: `"object"`
- <a id="s-6b4d3541d3"></a>`additionalProperties`: `false`
- <a id="s-4d2c0ff5f4"></a>`required`: `["work_id","phase"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-f6be725f42"></a>`branch_set_sha256` | no | anyOf=[(type="string"; pattern="^[0-9a-f]{64}$"); (type="null")]; default=null |  |
| <a id="s-10da4a1134"></a>`join_plan_sha256` | no | anyOf=[(type="string"; pattern="^[0-9a-f]{64}$"); (type="null")]; default=null |  |
| <a id="s-d99535311f"></a>`parent_work_id` | no | anyOf=[(type="string"; pattern="^[0-9a-f]{64}$"); (type="null")]; default=null |  |
| <a id="s-69e18a9ad4"></a>`phase` | yes | type="string"; enum=["eligible","claimed","observing","planning","target_preflight","queued","executing","output_finalizing","verifying","settled","retirement_pending","coordinating","abandon_pending","complete","inapplicable","failed","canceled"] |  |
| <a id="s-fac4d36641"></a>`work_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-dfaa67187b"></a>definition `WorkUpdatedEvent`

- <a id="s-cfbe61ceed"></a>`type`: `"object"`
- <a id="s-85ef992e48"></a>`additionalProperties`: `false`
- <a id="s-b64c13a5d3"></a>`required`: `["id","source","type","subject","time","data"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-277c57ceea"></a>`data` | yes | [WorkUpdatedEventData](#s-d34baf0de6) |  |
| <a id="s-d45652f268"></a>`datacontenttype` | no | type="string"; const="application/json"; default="application/json" |  |
| <a id="s-3502660ebf"></a>`id` | yes | type="string"; minLength=1 |  |
| <a id="s-06cb374f57"></a>`source` | yes | type="string"; const="urn:riverhog:stove0" |  |
| <a id="s-ef960eec2d"></a>`specversion` | no | type="string"; const="1.0"; default="1.0" |  |
| <a id="s-97aab70693"></a>`subject` | yes | type="string"; minLength=1 |  |
| <a id="s-3a99c2b253"></a>`time` | yes | type="string" |  |
| <a id="s-62185a28d0"></a>`type` | yes | type="string"; const="io.riverhog.stove0.work.updated" |  |

##### <a id="s-d34baf0de6"></a>definition `WorkUpdatedEventData`

- <a id="s-e41022a97e"></a>`type`: `"object"`
- <a id="s-0e3f93594d"></a>`additionalProperties`: `false`
- <a id="s-79f0188980"></a>`required`: `["work_id","phase","revision"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-adbe9c6258"></a>`phase` | yes | type="string"; enum=["eligible","claimed","observing","planning","target_preflight","queued","executing","output_finalizing","verifying","settled","retirement_pending","coordinating","abandon_pending","complete","inapplicable","failed","canceled"] |  |
| <a id="s-ac6f0e72c4"></a>`revision` | yes | type="integer"; minimum=2 |  |
| <a id="s-283df3316c"></a>`work_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

## Maintained corroboration

### Related interface records

- [require_progress_after](stove0-operator-contracts-stove0eventpage-require-progress-after.md)

## Governing policies

- <a id="pa-2b9942b22c"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-operator-contracts:stove0_operator_contracts](../../../evidence/sources/authorities.md#src-51ad84528d) — [some-implementations/stove0/packages/operator-contracts/src/stove0\_operator\_contracts/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/operator-contracts/src/stove0_operator_contracts/__init__.py)

### Machine authority

- `/external_contract/python/stove0_operator_contracts.Stove0EventPage`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: a166d7747cc25a404cfc9dd9df76af4fb0cc90e6efcbf0ec023fe1299867953c -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
      "$defs": {
        "BranchSetAdmittedEvent": {
          "additionalProperties": false,
          "properties": {
            "data": {
              "$ref": "#/$defs/BranchSetAdmittedEventData"
            },
            "datacontenttype": {
              "const": "application/json",
              "default": "application/json",
              "type": "string"
            },
            "id": {
              "minLength": 1,
              "type": "string"
            },
            "source": {
              "const": "urn:riverhog:stove0",
              "type": "string"
            },
            "specversion": {
              "const": "1.0",
              "default": "1.0",
              "type": "string"
            },
            "subject": {
              "minLength": 1,
              "type": "string"
            },
            "time": {
              "type": "string"
            },
            "type": {
              "const": "io.riverhog.stove0.branch-set.admitted",
              "type": "string"
            }
          },
          "required": [
            "id",
            "source",
            "type",
            "subject",
            "time",
            "data"
          ],
          "type": "object"
        },
        "BranchSetAdmittedEventData": {
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
        "EvaluationCreatedEvent": {
          "additionalProperties": false,
          "properties": {
            "data": {
              "$ref": "#/$defs/EvaluationCreatedEventData"
            },
            "datacontenttype": {
              "const": "application/json",
              "default": "application/json",
              "type": "string"
            },
            "id": {
              "minLength": 1,
              "type": "string"
            },
            "source": {
              "const": "urn:riverhog:stove0",
              "type": "string"
            },
            "specversion": {
              "const": "1.0",
              "default": "1.0",
              "type": "string"
            },
            "subject": {
              "minLength": 1,
              "type": "string"
            },
            "time": {
              "type": "string"
            },
            "type": {
              "const": "io.riverhog.stove0.evaluation.created",
              "type": "string"
            }
          },
          "required": [
            "id",
            "source",
            "type",
            "subject",
            "time",
            "data"
          ],
          "type": "object"
        },
        "EvaluationCreatedEventData": {
          "additionalProperties": false,
          "properties": {
            "evaluation_id": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "phase": {
              "enum": [
                "planning",
                "running",
                "partially_complete",
                "complete",
                "failed",
                "canceled"
              ],
              "type": "string"
            }
          },
          "required": [
            "evaluation_id",
            "phase"
          ],
          "type": "object"
        },
        "EvaluationUpdatedEvent": {
          "additionalProperties": false,
          "properties": {
            "data": {
              "$ref": "#/$defs/EvaluationUpdatedEventData"
            },
            "datacontenttype": {
              "const": "application/json",
              "default": "application/json",
              "type": "string"
            },
            "id": {
              "minLength": 1,
              "type": "string"
            },
            "source": {
              "const": "urn:riverhog:stove0",
              "type": "string"
            },
            "specversion": {
              "const": "1.0",
              "default": "1.0",
              "type": "string"
            },
            "subject": {
              "minLength": 1,
              "type": "string"
            },
            "time": {
              "type": "string"
            },
            "type": {
              "const": "io.riverhog.stove0.evaluation.updated",
              "type": "string"
            }
          },
          "required": [
            "id",
            "source",
            "type",
            "subject",
            "time",
            "data"
          ],
          "type": "object"
        },
        "EvaluationUpdatedEventData": {
          "additionalProperties": false,
          "properties": {
            "evaluation_id": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "phase": {
              "enum": [
                "planning",
                "running",
                "partially_complete",
                "complete",
                "failed",
                "canceled"
              ],
              "type": "string"
            },
            "revision": {
              "minimum": 2,
              "type": "integer"
            }
          },
          "required": [
            "evaluation_id",
            "phase",
            "revision"
          ],
          "type": "object"
        },
        "JoinAdmittedEvent": {
          "additionalProperties": false,
          "properties": {
            "data": {
              "$ref": "#/$defs/JoinAdmittedEventData"
            },
            "datacontenttype": {
              "const": "application/json",
              "default": "application/json",
              "type": "string"
            },
            "id": {
              "minLength": 1,
              "type": "string"
            },
            "source": {
              "const": "urn:riverhog:stove0",
              "type": "string"
            },
            "specversion": {
              "const": "1.0",
              "default": "1.0",
              "type": "string"
            },
            "subject": {
              "minLength": 1,
              "type": "string"
            },
            "time": {
              "type": "string"
            },
            "type": {
              "const": "io.riverhog.stove0.join.admitted",
              "type": "string"
            }
          },
          "required": [
            "id",
            "source",
            "type",
            "subject",
            "time",
            "data"
          ],
          "type": "object"
        },
        "JoinAdmittedEventData": {
          "additionalProperties": false,
          "properties": {
            "branch_set_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "join_plan_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "join_work_id": {
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
            "join_plan_sha256",
            "join_work_id"
          ],
          "type": "object"
        },
        "Stove0LifecycleEvent": {
          "discriminator": {
            "mapping": {
              "io.riverhog.stove0.branch-set.admitted": "#/$defs/BranchSetAdmittedEvent",
              "io.riverhog.stove0.evaluation.created": "#/$defs/EvaluationCreatedEvent",
              "io.riverhog.stove0.evaluation.updated": "#/$defs/EvaluationUpdatedEvent",
              "io.riverhog.stove0.join.admitted": "#/$defs/JoinAdmittedEvent",
              "io.riverhog.stove0.work.created": "#/$defs/WorkCreatedEvent",
              "io.riverhog.stove0.work.updated": "#/$defs/WorkUpdatedEvent"
            },
            "propertyName": "type"
          },
          "oneOf": [
            {
              "$ref": "#/$defs/WorkCreatedEvent"
            },
            {
              "$ref": "#/$defs/WorkUpdatedEvent"
            },
            {
              "$ref": "#/$defs/BranchSetAdmittedEvent"
            },
            {
              "$ref": "#/$defs/JoinAdmittedEvent"
            },
            {
              "$ref": "#/$defs/EvaluationCreatedEvent"
            },
            {
              "$ref": "#/$defs/EvaluationUpdatedEvent"
            }
          ]
        },
        "WorkCreatedEvent": {
          "additionalProperties": false,
          "properties": {
            "data": {
              "$ref": "#/$defs/WorkCreatedEventData"
            },
            "datacontenttype": {
              "const": "application/json",
              "default": "application/json",
              "type": "string"
            },
            "id": {
              "minLength": 1,
              "type": "string"
            },
            "source": {
              "const": "urn:riverhog:stove0",
              "type": "string"
            },
            "specversion": {
              "const": "1.0",
              "default": "1.0",
              "type": "string"
            },
            "subject": {
              "minLength": 1,
              "type": "string"
            },
            "time": {
              "type": "string"
            },
            "type": {
              "const": "io.riverhog.stove0.work.created",
              "type": "string"
            }
          },
          "required": [
            "id",
            "source",
            "type",
            "subject",
            "time",
            "data"
          ],
          "type": "object"
        },
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
                "retirement_pending",
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
        },
        "WorkUpdatedEvent": {
          "additionalProperties": false,
          "properties": {
            "data": {
              "$ref": "#/$defs/WorkUpdatedEventData"
            },
            "datacontenttype": {
              "const": "application/json",
              "default": "application/json",
              "type": "string"
            },
            "id": {
              "minLength": 1,
              "type": "string"
            },
            "source": {
              "const": "urn:riverhog:stove0",
              "type": "string"
            },
            "specversion": {
              "const": "1.0",
              "default": "1.0",
              "type": "string"
            },
            "subject": {
              "minLength": 1,
              "type": "string"
            },
            "time": {
              "type": "string"
            },
            "type": {
              "const": "io.riverhog.stove0.work.updated",
              "type": "string"
            }
          },
          "required": [
            "id",
            "source",
            "type",
            "subject",
            "time",
            "data"
          ],
          "type": "object"
        },
        "WorkUpdatedEventData": {
          "additionalProperties": false,
          "properties": {
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
                "retirement_pending",
                "coordinating",
                "abandon_pending",
                "complete",
                "inapplicable",
                "failed",
                "canceled"
              ],
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
            "revision"
          ],
          "type": "object"
        }
      },
      "additionalProperties": false,
      "properties": {
        "events": {
          "items": {
            "$ref": "#/$defs/Stove0LifecycleEvent"
          },
          "type": "array"
        },
        "has_more": {
          "type": "boolean"
        },
        "next_cursor": {
          "type": "string"
        }
      },
      "required": [
        "events",
        "next_cursor",
        "has_more"
      ],
      "type": "object"
    },
    "signature": "'(*, events: list[Stove0LifecycleEvent], next_cursor: str, has_more: bool) -> None'"
  },
  "distribution": "stove0-operator-contracts",
  "module": "stove0_operator_contracts",
  "name": "Stove0EventPage",
  "unit": "export"
}
```

</details>
