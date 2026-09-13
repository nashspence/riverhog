# stove0_protocol.WorkPayload

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-protocol:stove0-protocol-workpayload:fa6cad1292 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-d1d5916d09"></a>
| Field | Shape |
|---|---|
| <a id="s-a795c90df9"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-a0010a3270"></a>`distribution` | "stove0-protocol" |
| <a id="s-4ba5384cb7"></a>`module` | "stove0_protocol" |
| <a id="s-d71432a01d"></a>`name` | "WorkPayload" |
| <a id="s-c52c1c6d11"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [stove0_protocol.WorkPayload.canonical_inputs](stove0-protocol-workpayload-canonical-inputs.md)

## Governing policies

- <a id="pa-22c50e65a6"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-protocol:stove0_protocol](../../../evidence/sources.md#src-084138045e) — `reference/stove0/packages/protocol/src/stove0_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_protocol.WorkPayload`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: d553bf374adce2b18bb3f0cfa837f94494e9916610a4e56dfef7664dd93ad7e5 -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "c57f688649123bb4d7957af123bc8dce24ad0ff95884532522d0076ca01e702f",
    "signature": "\"(*, format: Literal['stove0-work/v1'] = 'stove0-work/v1', recipe: stove0_protocol.models.RecipeRef, inputs: Annotated[tuple[stove0_protocol.models.CollectionRootRef, ...], MinLen(min_length=1)], effective_intent: dict[str, JsonValue] = <factory>, evaluation: stove0_protocol.models.EvaluationBinding | None = None, fork_join: Optional[Annotated[stove0_protocol.models.BranchWorkBinding | stove0_protocol.models.JoinWorkBinding, FieldInfo(annotation=NoneType, required=True, discriminator='kind')]] = None) -> None\""
  },
  "distribution": "stove0-protocol",
  "module": "stove0_protocol",
  "name": "WorkPayload",
  "unit": "export"
}
```
