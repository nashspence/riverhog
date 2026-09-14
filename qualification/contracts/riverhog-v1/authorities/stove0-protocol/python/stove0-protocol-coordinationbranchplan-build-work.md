# stove0_protocol.CoordinationBranchPlan.build_work

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-protocol:stove0-protocol-coordinationbranchplan-build-work:ef9d046b6e -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-99a7519de3"></a>
- <a id="s-744e1049fd"></a>`distribution`: `stove0-protocol`
- <a id="s-60ecc0583e"></a>`module`: `stove0_protocol`
- <a id="s-456b7dd8bf"></a>`name`: `build_work`
- <a id="s-1243d7f138"></a>`owner`: `stove0_protocol.CoordinationBranchPlan`
- <a id="s-dc6f6e6d00"></a>`unit`: `member`

### Declared structure

- <a id="s-76c31aa524"></a>`kind`: `"classmethod"`
- <a id="s-1ba1da4104"></a>`signature`: `"\"(cls, *, parent_work: 'WorkIdentity', branch_id: 'str', decision_sha256: 'str', selection: 'ArtifactSelection', recipe: 'RecipeRef', effective_intent: 'Mapping[str, JsonValue]') -> 'WorkIdentity'\""`

## Maintained corroboration

### Related interface records

- [CoordinationBranchPlan](stove0-protocol-coordinationbranchplan.md)

## Governing policies

- <a id="pa-f7ce0d4b81"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-protocol:stove0_protocol](../../../evidence/sources.md#src-084138045e) — `reference/stove0/packages/protocol/src/stove0_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_protocol.CoordinationBranchPlan.build_work`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 9fd88155b5d9199c3267b28f248605b8e6fd067808e7ac32c7a8c546a145a04a -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, *, parent_work: 'WorkIdentity', branch_id: 'str', decision_sha256: 'str', selection: 'ArtifactSelection', recipe: 'RecipeRef', effective_intent: 'Mapping[str, JsonValue]') -> 'WorkIdentity'\""
  },
  "distribution": "stove0-protocol",
  "module": "stove0_protocol",
  "name": "build_work",
  "owner": "stove0_protocol.CoordinationBranchPlan",
  "unit": "member"
}
```
