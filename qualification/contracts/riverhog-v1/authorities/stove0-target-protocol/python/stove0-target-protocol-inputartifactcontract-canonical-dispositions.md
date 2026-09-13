# stove0_target_protocol.InputArtifactContract.canonical_dispositions

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-protocol:stove0-target-protocol-inputartifactcontr-e6c03b9529:4b3281362e -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-579d3ac6e4"></a>
| Field | Shape |
|---|---|
| <a id="s-53d3cf564a"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-f9066abf05"></a>`distribution` | "stove0-target-protocol" |
| <a id="s-ccbf7c01b1"></a>`module` | "stove0_target_protocol" |
| <a id="s-79c4ec3e17"></a>`name` | "canonical_dispositions" |
| <a id="s-c0ec185edf"></a>`owner` | "stove0_target_protocol.InputArtifactContract" |
| <a id="s-64e7cf3821"></a>`unit` | "member" |

## Maintained corroboration

### Related interface records

- [stove0_target_protocol.InputArtifactContract](stove0-target-protocol-inputartifactcontract.md)

## Governing policies

- <a id="pa-ac43187bd1"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-target-protocol:stove0_target_protocol](../../../evidence/sources.md#src-f4f0b22026) — `reference/stove0/packages/target-protocol/src/stove0_target_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_target_protocol.InputArtifactContract.canonical_dispositions`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 151d2be9671a78951cf5f1c16194d84e85451166c3d50ae9961b5386c1a2e5df -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, value: 'tuple[InputDisposition, ...] | None') -> 'tuple[InputDisposition, ...] | None'\""
  },
  "distribution": "stove0-target-protocol",
  "module": "stove0_target_protocol",
  "name": "canonical_dispositions",
  "owner": "stove0_target_protocol.InputArtifactContract",
  "unit": "member"
}
```
