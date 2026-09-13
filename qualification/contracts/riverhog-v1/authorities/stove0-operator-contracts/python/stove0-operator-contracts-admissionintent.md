# stove0_operator_contracts.AdmissionIntent

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-operator-contracts:stove0-operator-contracts-admissionintent:dd55b3eb2a -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-operator-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-7b4969ebdb"></a>
| Field | Shape |
|---|---|
| <a id="s-b7b3c75b59"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-b23fc88f3f"></a>`distribution` | "stove0-operator-contracts" |
| <a id="s-246851c59e"></a>`module` | "stove0_operator_contracts" |
| <a id="s-f16cf6aca7"></a>`name` | "AdmissionIntent" |
| <a id="s-800420742e"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [stove0_operator_contracts.AdmissionIntent.exact_identity](stove0-operator-contracts-admissionintent-exact-identity.md)
- [stove0_operator_contracts.AdmissionIntent.seal](stove0-operator-contracts-admissionintent-seal.md)

## Governing policies

- <a id="pa-66eae5b4ee"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-operator-contracts:stove0_operator_contracts](../../../evidence/sources.md#src-51ad84528d) — `reference/stove0/packages/operator-contracts/src/stove0_operator_contracts/__init__.py`

### Machine authority

- `/external_contract/python/stove0_operator_contracts.AdmissionIntent`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 796c06af610c241f16bba3a54ccb0e00d84ab9a4400b032b705ec43ab007ef99 -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "7a441cd9649c0ef9f6bb98d6e3d7ad782e0452a42d48382efff22f61ca6a3078",
    "signature": "\"(*, format: Literal['stove0-admission-intent/v1'] = 'stove0-admission-intent/v1', admission_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], policy_id: Annotated[str, MinLen(min_length=1), MaxLen(max_length=160)], policy_revision: Annotated[int, Ge(ge=1)], policy_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], required_tags: tuple[CollectionTag, ...], collection: riverhog_protocol.catalog_sync.CatalogSyncDescriptor, recipe_id: Annotated[str, MinLen(min_length=1), MaxLen(max_length=160)], recipe_revision: Annotated[int, Ge(ge=1)], recipe_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], effective_intent: dict[str, JsonValue]) -> None\""
  },
  "distribution": "stove0-operator-contracts",
  "module": "stove0_operator_contracts",
  "name": "AdmissionIntent",
  "unit": "export"
}
```
