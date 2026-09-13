# stove0_core.EvaluationChild

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-evaluationchild:a4ecbd76ce -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-9dec0e3508"></a>
| Field | Shape |
|---|---|
| <a id="s-fb7272c0b5"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-974333000c"></a>`distribution` | "stove0-server" |
| <a id="s-22cc756702"></a>`module` | "stove0_core" |
| <a id="s-d93d9fefff"></a>`name` | "EvaluationChild" |
| <a id="s-57ff840119"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [stove0_core.EvaluationChild.validate_output](stove0-core-evaluationchild-validate-output.md)

## Governing policies

- <a id="pa-c6c927329e"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-server:stove0_core](../../../evidence/sources.md#src-7558b08e7f) — `reference/stove0/application/server/src/stove0_core/__init__.py`

### Machine authority

- `/external_contract/python/stove0_core.EvaluationChild`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 21948efe9d99c37308f80e3fd7eb738d625e44bd93709fb0e724a118837c2b11 -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "1b83214578ce9b5a5a7e03e493454b0f3b2ad79ad49a24cc8885e4ca80265a0e",
    "signature": "\"(*, variant_id: Annotated[str, MinLen(min_length=1), MaxLen(max_length=160)], work_id: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')], state: Literal['pending', 'active', 'complete', 'inapplicable', 'failed', 'canceled'] = 'pending', output: stove0_target_protocol.protocol.OutputCollectionRef | None = None) -> None\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "EvaluationChild",
  "unit": "export"
}
```
