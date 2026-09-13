# stove0_target_support.InputArtifactContract.canonical_dispositions

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-support:stove0-target-support-inputartifactcontra-c9efa31b8f:6e0cdbb946 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-4346df3327"></a>
| Field | Shape |
|---|---|
| <a id="s-20892d9f1a"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-f7d80d0f8c"></a>`distribution` | "stove0-target-support" |
| <a id="s-19e377207b"></a>`module` | "stove0_target_support" |
| <a id="s-869cb26190"></a>`name` | "canonical_dispositions" |
| <a id="s-aa70368728"></a>`owner` | "stove0_target_support.InputArtifactContract" |
| <a id="s-f058809ff6"></a>`unit` | "member" |

## Maintained corroboration

### Related interface records

- [stove0_target_support.InputArtifactContract](stove0-target-support-inputartifactcontract.md)

## Governing policies

- <a id="pa-d8a50c5551"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-target-support:stove0_target_support](../../../evidence/sources.md#src-3c01163237) — `reference/stove0/packages/target-support/src/stove0_target_support/__init__.py`

### Machine authority

- `/external_contract/python/stove0_target_support.InputArtifactContract.canonical_dispositions`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: cd4d90cc1315d765d1badf6fafd8322ea424f6dba6dff08dd3beace7a779705e -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, value: 'tuple[InputDisposition, ...] | None') -> 'tuple[InputDisposition, ...] | None'\""
  },
  "distribution": "stove0-target-support",
  "module": "stove0_target_support",
  "name": "canonical_dispositions",
  "owner": "stove0_target_support.InputArtifactContract",
  "unit": "member"
}
```
