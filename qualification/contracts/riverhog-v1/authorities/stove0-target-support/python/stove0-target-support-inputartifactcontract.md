# stove0_target_support.InputArtifactContract

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-support:stove0-target-support-inputartifactcontract:0fd9beb37e -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-3516fcfc81"></a>
| Field | Shape |
|---|---|
| <a id="s-b0629f3e30"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-7f0781c46a"></a>`distribution` | "stove0-target-support" |
| <a id="s-e1d2ca92f4"></a>`module` | "stove0_target_support" |
| <a id="s-690e7ca91c"></a>`name` | "InputArtifactContract" |
| <a id="s-e5b11593a6"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [stove0_target_support.InputArtifactContract.validate_cardinality](stove0-target-support-inputartifactcontract-validate-cardinality.md)
- [stove0_target_support.InputArtifactContract.canonical_dispositions](stove0-target-support-inputartifactcontract-canonical-dispositions.md)

## Governing policies

- <a id="pa-99db522fb9"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-target-support:stove0_target_support](../../../evidence/sources.md#src-3c01163237) — `reference/stove0/packages/target-support/src/stove0_target_support/__init__.py`

### Machine authority

- `/external_contract/python/stove0_target_support.InputArtifactContract`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 8af38017d290e3303754b07afccaf46577270520b1564367988d83995592efcb -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "a77940dd0cdd6154e96246459b292378e83a95b94bd488f79c5ead938fb2a73c",
    "signature": "\"(*, role: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], minimum: Annotated[int, Ge(ge=0)] = 1, maximum: Annotated[int | None, Ge(ge=1)] = None, allowed_dispositions: tuple[typing.Literal['transformed', 'preserved', 'omitted', 'rejected'], ...] | None = None) -> None\""
  },
  "distribution": "stove0-target-support",
  "module": "stove0_target_support",
  "name": "InputArtifactContract",
  "unit": "export"
}
```
