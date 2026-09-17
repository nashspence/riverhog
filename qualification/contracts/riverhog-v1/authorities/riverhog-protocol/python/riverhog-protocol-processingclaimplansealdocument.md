# riverhog_protocol.ProcessingClaimPlanSealDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-processingclaimplansealdocument:87a3413aae -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-071ecf24c0"></a>
- <a id="s-f320099d6d"></a>`distribution`: `riverhog-protocol`
- <a id="s-ef581ea454"></a>`module`: `riverhog_protocol`
- <a id="s-4ba730e043"></a>`name`: `ProcessingClaimPlanSealDocument`
- <a id="s-73111f7c5a"></a>`unit`: `export`

### Declared structure

- <a id="s-e30f6c29f8"></a>`kind`: `"class"`
- <a id="s-b97d8a7237"></a>`signature`: `"\"(*, fence: Annotated[int, Ge(ge=1)], execution_id: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')], controller_evidence: dict[str, typing.Any], controller_evidence_sha256: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')], operation: riverhog_protocol.collection_workflow_transport.OperationIdentityDocument, retirement_policy: Literal['retain', 'retire-after-verified-output'] = 'retain', retirement_grace_seconds: Annotated[int, Ge(ge=0)] = 0) -> None\""`

#### Validated model schema

<a id="s-5166053768"></a>

- <a id="s-2cf7e6847f"></a>`type`: `"object"`
- <a id="s-95df9fb5b4"></a>`additionalProperties`: `false`
- `if`: [See `if`](#s-1c58018aaa)
- <a id="s-accf47b326"></a>`required`: `["fence","execution_id","controller_evidence","controller_evidence_sha256","operation"]`
- `then`: [See `then`](#s-77762ccc0f)

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-24f056ab1a"></a>`controller_evidence` | yes | type="object"; additionalProperties=(any JSON value); x-riverhog-encoded-bytes-max=16777216; x-riverhog-extent={"policy":"contract_max","reason":"bounded-controller-evidence-envelope"} |  |
| <a id="s-bae704d1c6"></a>`controller_evidence_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-18261de791"></a>`execution_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-6fbb64b834"></a>`fence` | yes | type="integer"; minimum=1 |  |
| <a id="s-2549d87071"></a>`operation` | yes | [OperationIdentityDocument](#s-fa7d9bfff2) |  |
| <a id="s-3a0b445d63"></a>`retirement_grace_seconds` | no | type="integer"; minimum=0; default=0 |  |
| <a id="s-e1c4c56d90"></a>`retirement_policy` | no | type="string"; enum=["retain","retire-after-verified-output"]; default="retain" |  |

##### Definitions

- [OperationIdentityDocument](#s-fa7d9bfff2)

##### <a id="s-1c58018aaa"></a>`if`


###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-de210e83a6"></a>`retirement_policy` | no | const="retain" |  |

##### <a id="s-77762ccc0f"></a>`then`


###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-e6ecc4b8c2"></a>`retirement_grace_seconds` | no | const=0 |  |

##### <a id="s-fa7d9bfff2"></a>definition `OperationIdentityDocument`

- <a id="s-71e0f2bcc1"></a>`type`: `"object"`
- <a id="s-8ff32c8618"></a>`additionalProperties`: `false`
- <a id="s-eee6fe2a10"></a>`required`: `["id","sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-ecf17275ab"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-cdfa89074a"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

## Maintained corroboration

### Related interface records

- [__getitem__](riverhog-protocol-processingclaimplansealdocument-getitem.md)
- [validate_plan](riverhog-protocol-processingclaimplansealdocument-validate-plan.md)
- [get](riverhog-protocol-processingclaimplansealdocument-get.md)

## Governing policies

- <a id="pa-66241af7b9"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources.md#src-19e35f15d9) — [packages/riverhog-protocol/src/riverhog\_protocol/\_\_init\_\_.py](../../../../../../packages/riverhog-protocol/src/riverhog_protocol/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_protocol.ProcessingClaimPlanSealDocument`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: e1761f489d43d41758fbc6172a0c5784ddb3e3864f5f7a7b813147b127197a52 -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
      "$defs": {
        "OperationIdentityDocument": {
          "additionalProperties": false,
          "properties": {
            "id": {
              "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
              "type": "string"
            },
            "sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            }
          },
          "required": [
            "id",
            "sha256"
          ],
          "type": "object"
        }
      },
      "additionalProperties": false,
      "if": {
        "properties": {
          "retirement_policy": {
            "const": "retain"
          }
        }
      },
      "properties": {
        "controller_evidence": {
          "additionalProperties": true,
          "type": "object",
          "x-riverhog-encoded-bytes-max": 16777216,
          "x-riverhog-extent": {
            "policy": "contract_max",
            "reason": "bounded-controller-evidence-envelope"
          }
        },
        "controller_evidence_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "type": "string"
        },
        "execution_id": {
          "pattern": "^[0-9a-f]{64}$",
          "type": "string"
        },
        "fence": {
          "minimum": 1,
          "type": "integer"
        },
        "operation": {
          "$ref": "#/$defs/OperationIdentityDocument"
        },
        "retirement_grace_seconds": {
          "default": 0,
          "minimum": 0,
          "type": "integer"
        },
        "retirement_policy": {
          "default": "retain",
          "enum": [
            "retain",
            "retire-after-verified-output"
          ],
          "type": "string"
        }
      },
      "required": [
        "fence",
        "execution_id",
        "controller_evidence",
        "controller_evidence_sha256",
        "operation"
      ],
      "then": {
        "properties": {
          "retirement_grace_seconds": {
            "const": 0
          }
        }
      },
      "type": "object"
    },
    "signature": "\"(*, fence: Annotated[int, Ge(ge=1)], execution_id: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')], controller_evidence: dict[str, typing.Any], controller_evidence_sha256: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')], operation: riverhog_protocol.collection_workflow_transport.OperationIdentityDocument, retirement_policy: Literal['retain', 'retire-after-verified-output'] = 'retain', retirement_grace_seconds: Annotated[int, Ge(ge=0)] = 0) -> None\""
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "ProcessingClaimPlanSealDocument",
  "unit": "export"
}
```

</details>
