# stove0_target_support.OperationContractPayload

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-support:stove0-target-support-operationcontractpayload:89e8287214 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-3ba167db03"></a>
| Field | Shape |
|---|---|
| <a id="s-569471e9cd"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-8f54560d67"></a>`distribution` | "stove0-target-support" |
| <a id="s-f507db26e5"></a>`module` | "stove0_target_support" |
| <a id="s-380f17074d"></a>`name` | "OperationContractPayload" |
| <a id="s-c0c49bf4d5"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [stove0_target_support.OperationContractPayload.validate_roles](stove0-target-support-operationcontractpayload-validate-roles.md)
- [stove0_target_support.OperationContractPayload.bind_semantic_conformance_vectors](stove0-target-support-operationcontractpayload-bind-semantic-conformance-vectors.md)

## Governing policies

- <a id="pa-4c129baa8a"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-target-support:stove0_target_support](../../../evidence/sources.md#src-3c01163237) — `reference/stove0/packages/target-support/src/stove0_target_support/__init__.py`

### Machine authority

- `/external_contract/python/stove0_target_support.OperationContractPayload`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 137454eb32e377a0b990994ff6824ae718ee1ec4226c623a8441b74b64444b98 -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "81f42d05e99391558a8842214e784f88daef258d34fe0579d6cdb9ca136913db",
    "signature": "\"(*, id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], result_kind: Literal['collection', 'external-effect'] = 'collection', intent_schema: stove0_protocol.models.JsonSchemaDocument, intent_semantics: stove0_protocol.models.SemanticValidationProfile, inputs: Annotated[tuple[stove0_target_protocol.protocol.InputArtifactContract, ...], MinLen(min_length=1)], outputs: tuple[stove0_target_protocol.protocol.OutputArtifactContract, ...] = (), effect_receipt_schema: stove0_protocol.models.JsonSchemaDocument | None = None, source_retirement_permitted: bool = False) -> None\""
  },
  "distribution": "stove0-target-support",
  "module": "stove0_target_support",
  "name": "OperationContractPayload",
  "unit": "export"
}
```
