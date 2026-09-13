# stove0_observer_protocol.ObserverContractSupport

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-observer-protocol:stove0-observer-protocol-observercontractsupport:6ab5904f17 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-observer-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-0c4092e9ce"></a>
| Field | Shape |
|---|---|
| <a id="s-678b64aeac"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-ce4449aac9"></a>`distribution` | "stove0-observer-protocol" |
| <a id="s-c6a128bcdc"></a>`module` | "stove0_observer_protocol" |
| <a id="s-211057bac1"></a>`name` | "ObserverContractSupport" |
| <a id="s-a5c8b802a9"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [stove0_observer_protocol.ObserverContractSupport.from_contract](stove0-observer-protocol-observercontractsupport-from-contract.md)

## Governing policies

- <a id="pa-44784ea4a6"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-observer-protocol:stove0_observer_protocol](../../../evidence/sources.md#src-62450e0156) — `reference/stove0/packages/observer-protocol/src/stove0_observer_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_observer_protocol.ObserverContractSupport`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: a96410136d95470f1f83c3317d765bfb52d01acfa77a09d004f5d16effaba86c -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "fab7dc8d2c5290e8e1fc0607fffbbdea2505809a70a027f3e882712313fada0b",
    "signature": "\"(*, contract_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], contract_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], options_schema: stove0_protocol.models.JsonSchemaDocument, facts_schema: stove0_protocol.models.JsonSchemaDocument, facts_semantics: stove0_protocol.models.SemanticValidationProfile, preferred_subject_batch_size: Annotated[int, Ge(ge=1)] = 128, maximum_result_bytes: Annotated[int, Ge(ge=1), Le(le=67108864)]) -> None\""
  },
  "distribution": "stove0-observer-protocol",
  "module": "stove0_observer_protocol",
  "name": "ObserverContractSupport",
  "unit": "export"
}
```
