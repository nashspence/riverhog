# stove0_operator_contracts.AdmissionPolicy

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-operator-contracts:stove0-operator-contracts-admissionpolicy:37fbbc74d4 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-operator-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-b469ad5ed5"></a>
| Field | Shape |
|---|---|
| <a id="s-d7ac0347f7"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-6c74bb7c21"></a>`distribution` | "stove0-operator-contracts" |
| <a id="s-e11f6dbc15"></a>`module` | "stove0_operator_contracts" |
| <a id="s-85d262fff3"></a>`name` | "AdmissionPolicy" |
| <a id="s-027fdcf56d"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [stove0_operator_contracts.AdmissionPolicy.canonical_required_tags](stove0-operator-contracts-admissionpolicy-canonical-required-tags.md)
- [stove0_operator_contracts.AdmissionPolicy.policy_sha256](stove0-operator-contracts-admissionpolicy-policy-sha256.md)

## Governing policies

- <a id="pa-ce091d4582"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-operator-contracts:stove0_operator_contracts](../../../evidence/sources.md#src-51ad84528d) — `reference/stove0/packages/operator-contracts/src/stove0_operator_contracts/__init__.py`

### Machine authority

- `/external_contract/python/stove0_operator_contracts.AdmissionPolicy`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: f0461759ff7bb6789b9ab197105d96bcbfa4ac6c3abaf0af31722d391c0cd12d -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "5751b77c2fcfd5f1f89e28c406af3c3c97713360ffb17dbcfaa02a2c2f414907",
    "signature": "\"(*, format: Literal['stove0-admission-policy/v1'] = 'stove0-admission-policy/v1', id: Annotated[str, _PydanticGeneralMetadata(pattern='^[a-z0-9]\u0028?:[a-z0-9._-]{0,158}[a-z0-9])?$')], revision: Annotated[int, Ge(ge=1)], required_tags: Annotated[tuple[CollectionTag, ...], MinLen(min_length=1), MaxLen(max_length=100)], recipe_id: Annotated[str, MinLen(min_length=1), MaxLen(max_length=160)], recipe_revision: Annotated[int, Ge(ge=1)], recipe_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], effective_intent: dict[str, JsonValue] = <factory>, automatic_preview: Literal['accept-ready'] = 'accept-ready') -> None\""
  },
  "distribution": "stove0-operator-contracts",
  "module": "stove0_operator_contracts",
  "name": "AdmissionPolicy",
  "unit": "export"
}
```
