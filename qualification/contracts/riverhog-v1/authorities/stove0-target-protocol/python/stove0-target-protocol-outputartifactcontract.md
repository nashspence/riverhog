# stove0_target_protocol.OutputArtifactContract

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-protocol:stove0-target-protocol-outputartifactcontract:8196403d36 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-0ce80a8e84"></a>
| Field | Shape |
|---|---|
| <a id="s-6de6c6888a"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-5250210799"></a>`distribution` | "stove0-target-protocol" |
| <a id="s-a6767449e9"></a>`module` | "stove0_target_protocol" |
| <a id="s-2f67eb294a"></a>`name` | "OutputArtifactContract" |
| <a id="s-cb47db24a9"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [stove0_target_protocol.OutputArtifactContract.unique_roles](stove0-target-protocol-outputartifactcontract-unique-roles.md)
- [stove0_target_protocol.OutputArtifactContract.validate_cardinality](stove0-target-protocol-outputartifactcontract-validate-cardinality.md)

## Governing policies

- <a id="pa-e48854c526"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-target-protocol:stove0_target_protocol](../../../evidence/sources.md#src-f4f0b22026) — `reference/stove0/packages/target-protocol/src/stove0_target_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_target_protocol.OutputArtifactContract`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 763ae48b71e76d004fd550484cc7ee5a0a7fcf1399c3f4275f965bf0841aa707 -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "4f754cfeb5d4d5d0a51d99f9a9d9d066ad7a0517a1a2dd82862bd72810813f4e",
    "signature": "\"(*, role: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], minimum: Annotated[int, Ge(ge=0)] = 1, maximum: Annotated[int | None, Ge(ge=1)] = None, derived_from_roles: Annotated[tuple[Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], ...], MinLen(min_length=1)]) -> None\""
  },
  "distribution": "stove0-target-protocol",
  "module": "stove0_target_protocol",
  "name": "OutputArtifactContract",
  "unit": "export"
}
```
