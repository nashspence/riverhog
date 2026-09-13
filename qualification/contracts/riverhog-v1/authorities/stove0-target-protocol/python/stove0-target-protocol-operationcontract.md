# stove0_target_protocol.OperationContract

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-protocol:stove0-target-protocol-operationcontract:ef9259208b -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-05e475df5c"></a>
| Field | Shape |
|---|---|
| <a id="s-f6da62df10"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-7acbbf9d43"></a>`distribution` | "stove0-target-protocol" |
| <a id="s-3cda6243e6"></a>`module` | "stove0_target_protocol" |
| <a id="s-f96be9133e"></a>`name` | "OperationContract" |
| <a id="s-379aa4d20d"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [stove0_target_protocol.OperationContract.verify_digest](stove0-target-protocol-operationcontract-verify-digest.md)
- [stove0_target_protocol.OperationContract.seal](stove0-target-protocol-operationcontract-seal.md)

## Governing policies

- <a id="pa-85c4934a60"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-target-protocol:stove0_target_protocol](../../../evidence/sources.md#src-f4f0b22026) — `reference/stove0/packages/target-protocol/src/stove0_target_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_target_protocol.OperationContract`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 4bb7672e3dbbddc1e7d8185eccb00c135978b95d739d99dce9ce8bfb5feb918a -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "aa2a5d676e27f1328d9ac0508bb514c28355ff594b7e72c052731d4fbc4edd22",
    "signature": "\"(*, id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], result_kind: Literal['collection', 'external-effect'] = 'collection', intent_schema: stove0_protocol.models.JsonSchemaDocument, intent_semantics: stove0_protocol.models.SemanticValidationProfile, inputs: Annotated[tuple[stove0_target_protocol.protocol.InputArtifactContract, ...], MinLen(min_length=1)], outputs: tuple[stove0_target_protocol.protocol.OutputArtifactContract, ...] = (), effect_receipt_schema: stove0_protocol.models.JsonSchemaDocument | None = None, source_retirement_permitted: bool = False, contract_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]) -> None\""
  },
  "distribution": "stove0-target-protocol",
  "module": "stove0_target_protocol",
  "name": "OperationContract",
  "unit": "export"
}
```
