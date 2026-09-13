# stove0_operator_contracts.WorkflowPreviewIn

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-operator-contracts:stove0-operator-contracts-workflowpreviewin:58e3423444 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-operator-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-09bdb29e6d"></a>
| Field | Shape |
|---|---|
| <a id="s-74ed0bcbc1"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-76c97a5b30"></a>`distribution` | "stove0-operator-contracts" |
| <a id="s-9b585e3eaa"></a>`module` | "stove0_operator_contracts" |
| <a id="s-8846457f5f"></a>`name` | "WorkflowPreviewIn" |
| <a id="s-ad054de7e7"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [stove0_operator_contracts.WorkflowPreviewIn.canonical_inputs](stove0-operator-contracts-workflowpreviewin-canonical-inputs.md)

## Governing policies

- <a id="pa-acc4cce580"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-operator-contracts:stove0_operator_contracts](../../../evidence/sources.md#src-51ad84528d) — `reference/stove0/packages/operator-contracts/src/stove0_operator_contracts/__init__.py`

### Machine authority

- `/external_contract/python/stove0_operator_contracts.WorkflowPreviewIn`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: a226e5f81eeb25759ac3faa0b5fd682aca2a8214ce8136fa5ed0d32780de2279 -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "d481dd8506429b4854eac08e0da41af254197bca3e7951499e90d837ad076ba9",
    "signature": "'(*, recipe_id: Annotated[str, MinLen(min_length=1), MaxLen(max_length=160)], recipe_revision: Annotated[int | None, Ge(ge=1)] = None, inputs: Annotated[tuple[stove0_protocol.models.CollectionRootRef, ...], MinLen(min_length=1)], effective_intent: dict[str, JsonValue] = <factory>) -> None'"
  },
  "distribution": "stove0-operator-contracts",
  "module": "stove0_operator_contracts",
  "name": "WorkflowPreviewIn",
  "unit": "export"
}
```
