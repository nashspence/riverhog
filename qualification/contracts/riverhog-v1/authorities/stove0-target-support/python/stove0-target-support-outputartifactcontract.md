# stove0_target_support.OutputArtifactContract

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-support:stove0-target-support-outputartifactcontract:b3db05b34b -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-272d53846e"></a>
| Field | Shape |
|---|---|
| <a id="s-c9ce348c73"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-bb45048b43"></a>`distribution` | "stove0-target-support" |
| <a id="s-b4b0a9aa77"></a>`module` | "stove0_target_support" |
| <a id="s-9a1264036c"></a>`name` | "OutputArtifactContract" |
| <a id="s-8f23b1dc99"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [stove0_target_support.OutputArtifactContract.unique_roles](stove0-target-support-outputartifactcontract-unique-roles.md)
- [stove0_target_support.OutputArtifactContract.validate_cardinality](stove0-target-support-outputartifactcontract-validate-cardinality.md)

## Governing policies

- <a id="pa-885af9e524"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-target-support:stove0_target_support](../../../evidence/sources.md#src-3c01163237) — `reference/stove0/packages/target-support/src/stove0_target_support/__init__.py`

### Machine authority

- `/external_contract/python/stove0_target_support.OutputArtifactContract`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 19dc94a180360c39b59a40fb8bed22e7a09d86f177ba0a8911db72851ef63cd7 -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "4f754cfeb5d4d5d0a51d99f9a9d9d066ad7a0517a1a2dd82862bd72810813f4e",
    "signature": "\"(*, role: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], minimum: Annotated[int, Ge(ge=0)] = 1, maximum: Annotated[int | None, Ge(ge=1)] = None, derived_from_roles: Annotated[tuple[Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], ...], MinLen(min_length=1)]) -> None\""
  },
  "distribution": "stove0-target-support",
  "module": "stove0_target_support",
  "name": "OutputArtifactContract",
  "unit": "export"
}
```
