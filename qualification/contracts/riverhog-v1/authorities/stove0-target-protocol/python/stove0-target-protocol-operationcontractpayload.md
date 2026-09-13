# stove0_target_protocol.OperationContractPayload

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-protocol:stove0-target-protocol-operationcontractpayload:572ce2bea7 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-48a8dc0c02"></a>
| Field | Shape |
|---|---|
| <a id="s-fc52a7d5b9"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-7286bd54a6"></a>`distribution` | "stove0-target-protocol" |
| <a id="s-d6d4ac5329"></a>`module` | "stove0_target_protocol" |
| <a id="s-0f64d6c859"></a>`name` | "OperationContractPayload" |
| <a id="s-8e8d398f9b"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [stove0_target_protocol.OperationContractPayload.validate_roles](stove0-target-protocol-operationcontractpayload-validate-roles.md)
- [stove0_target_protocol.OperationContractPayload.bind_semantic_conformance_vectors](stove0-target-protocol-operationcontractpayload-bind-semantic-conformance-vectors.md)

## Governing policies

- <a id="pa-572eff2d20"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-target-protocol:stove0_target_protocol](../../../evidence/sources.md#src-f4f0b22026) — `reference/stove0/packages/target-protocol/src/stove0_target_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_target_protocol.OperationContractPayload`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 4f799b98a11f2bd61a2d2d768216a854665a0fff29464d63e444894dbe3984c1 -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "81f42d05e99391558a8842214e784f88daef258d34fe0579d6cdb9ca136913db",
    "signature": "\"(*, id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], result_kind: Literal['collection', 'external-effect'] = 'collection', intent_schema: stove0_protocol.models.JsonSchemaDocument, intent_semantics: stove0_protocol.models.SemanticValidationProfile, inputs: Annotated[tuple[stove0_target_protocol.protocol.InputArtifactContract, ...], MinLen(min_length=1)], outputs: tuple[stove0_target_protocol.protocol.OutputArtifactContract, ...] = (), effect_receipt_schema: stove0_protocol.models.JsonSchemaDocument | None = None, source_retirement_permitted: bool = False) -> None\""
  },
  "distribution": "stove0-target-protocol",
  "module": "stove0_target_protocol",
  "name": "OperationContractPayload",
  "unit": "export"
}
```
