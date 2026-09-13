# stove0_target_protocol.OutputSourceEdge

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-protocol:stove0-target-protocol-outputsourceedge:5c2289e37c -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-d8c2311ad4"></a>
| Field | Shape |
|---|---|
| <a id="s-aecb5852e6"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-3f231bd04c"></a>`distribution` | "stove0-target-protocol" |
| <a id="s-1c35b4340d"></a>`module` | "stove0_target_protocol" |
| <a id="s-fa14dbaa12"></a>`name` | "OutputSourceEdge" |
| <a id="s-8cfaa3011a"></a>`unit` | "export" |

## Governing policies

- <a id="pa-a9de7f84fe"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-target-protocol:stove0_target_protocol](../../../evidence/sources.md#src-f4f0b22026) — `reference/stove0/packages/target-protocol/src/stove0_target_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_target_protocol.OutputSourceEdge`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: c62c4cacf18cb2b2a9605f1659a6e69254de20c08dec7546b13a738d5102e5d4 -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "972b082324ce99560dcf290aaa2190dca0d8d4ad38ed3ae820655588e94db2f4",
    "signature": "\"(*, output_id: Annotated[str, _PydanticGeneralMetadata(pattern='^[A-Za-z0-9]\u0028?:[A-Za-z0-9._-]{0,158}[A-Za-z0-9])?$')], input_id: Annotated[str, _PydanticGeneralMetadata(pattern='^[A-Za-z0-9]\u0028?:[A-Za-z0-9._-]{0,158}[A-Za-z0-9])?$')]) -> None\""
  },
  "distribution": "stove0-target-protocol",
  "module": "stove0_target_protocol",
  "name": "OutputSourceEdge",
  "unit": "export"
}
```
