# stove0_target_protocol.OutputArtifactSetIdentity.validate_summary

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-protocol:stove0-target-protocol-outputartifactseti-229bbe8434:0f9b559f62 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-ac084beaf7"></a>
| Field | Shape |
|---|---|
| <a id="s-afa72eaca1"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-33d6e33158"></a>`distribution` | "stove0-target-protocol" |
| <a id="s-5e89a26c40"></a>`module` | "stove0_target_protocol" |
| <a id="s-3481895b98"></a>`name` | "validate_summary" |
| <a id="s-f5fde0c5c7"></a>`owner` | "stove0_target_protocol.OutputArtifactSetIdentity" |
| <a id="s-e3a56174e9"></a>`unit` | "member" |

## Maintained corroboration

### Related interface records

- [stove0_target_protocol.OutputArtifactSetIdentity](stove0-target-protocol-outputartifactsetidentity.md)

## Governing policies

- <a id="pa-5457b004e5"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-target-protocol:stove0_target_protocol](../../../evidence/sources.md#src-f4f0b22026) — `reference/stove0/packages/target-protocol/src/stove0_target_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_target_protocol.OutputArtifactSetIdentity.validate_summary`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: ef4e2ec1bf86f42693afeff4315dc4fade87ba30d1750bbc8f6bac323a27df8f -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'Self'\""
  },
  "distribution": "stove0-target-protocol",
  "module": "stove0_target_protocol",
  "name": "validate_summary",
  "owner": "stove0_target_protocol.OutputArtifactSetIdentity",
  "unit": "member"
}
```
