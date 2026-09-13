# stove0_target_support.OperationContract

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-support:stove0-target-support-operationcontract:9b335ba1b0 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-6f90eb7812"></a>
| Field | Shape |
|---|---|
| <a id="s-0418eb0311"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-934ffb10b9"></a>`distribution` | "stove0-target-support" |
| <a id="s-96fae777c7"></a>`module` | "stove0_target_support" |
| <a id="s-32fd9a4101"></a>`name` | "OperationContract" |
| <a id="s-cc1165a6cd"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [stove0_target_support.OperationContract.seal](stove0-target-support-operationcontract-seal.md)
- [stove0_target_support.OperationContract.verify_digest](stove0-target-support-operationcontract-verify-digest.md)

## Governing policies

- <a id="pa-5b5e99398e"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-target-support:stove0_target_support](../../../evidence/sources.md#src-3c01163237) — `reference/stove0/packages/target-support/src/stove0_target_support/__init__.py`

### Machine authority

- `/external_contract/python/stove0_target_support.OperationContract`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 6339331ea1ee543e8fe761c74f0dd833d8e41506f15c15d2e0763f2215f7fbb7 -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "aa2a5d676e27f1328d9ac0508bb514c28355ff594b7e72c052731d4fbc4edd22",
    "signature": "\"(*, id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], result_kind: Literal['collection', 'external-effect'] = 'collection', intent_schema: stove0_protocol.models.JsonSchemaDocument, intent_semantics: stove0_protocol.models.SemanticValidationProfile, inputs: Annotated[tuple[stove0_target_protocol.protocol.InputArtifactContract, ...], MinLen(min_length=1)], outputs: tuple[stove0_target_protocol.protocol.OutputArtifactContract, ...] = (), effect_receipt_schema: stove0_protocol.models.JsonSchemaDocument | None = None, source_retirement_permitted: bool = False, contract_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]) -> None\""
  },
  "distribution": "stove0-target-support",
  "module": "stove0_target_support",
  "name": "OperationContract",
  "unit": "export"
}
```
