# stove0_operator_contracts.AdmissionPolicyStatus

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-operator-contracts:stove0-operator-contracts-admissionpolicystatus:07533add16 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-operator-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-a53f1e89da"></a>
| Field | Shape |
|---|---|
| <a id="s-afc793dbff"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-18e3233808"></a>`distribution` | "stove0-operator-contracts" |
| <a id="s-9e6f450fff"></a>`module` | "stove0_operator_contracts" |
| <a id="s-6608828010"></a>`name` | "AdmissionPolicyStatus" |
| <a id="s-b35967e0e6"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [stove0_operator_contracts.AdmissionPolicyStatus.exact_policy](stove0-operator-contracts-admissionpolicystatus-exact-policy.md)

## Governing policies

- <a id="pa-e69858dd95"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-operator-contracts:stove0_operator_contracts](../../../evidence/sources.md#src-51ad84528d) — `reference/stove0/packages/operator-contracts/src/stove0_operator_contracts/__init__.py`

### Machine authority

- `/external_contract/python/stove0_operator_contracts.AdmissionPolicyStatus`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: ebec4902282499c842c0fb9cf2d52070a77470526bbff9762addc70b563392a8 -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "5f0c6158276ff3ee0f2695bf26ec3c7aa12e1b9341146ab950503d5efdf49ada",
    "signature": "\"(*, policy: stove0_operator_contracts.AdmissionPolicy, policy_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], phase: Literal['new', 'baseline', 'following', 'reset_required'], source_identity: Optional[Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]] = None, authorization_view_identity: Optional[Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]] = None, baseline_mode: Literal['observe', 'backfill'], through_revision: Annotated[str, _PydanticGeneralMetadata(pattern='^(?:0|[1-9][0-9]*)$')], updated_at: Annotated[str, MinLen(min_length=1), MaxLen(max_length=40)]) -> None\""
  },
  "distribution": "stove0-operator-contracts",
  "module": "stove0_operator_contracts",
  "name": "AdmissionPolicyStatus",
  "unit": "export"
}
```
