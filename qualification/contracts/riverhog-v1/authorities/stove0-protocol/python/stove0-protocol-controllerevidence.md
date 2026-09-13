# stove0_protocol.ControllerEvidence

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-protocol:stove0-protocol-controllerevidence:2b8e594eda -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-3811541496"></a>
| Field | Shape |
|---|---|
| <a id="s-60c0377fb1"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-352d403811"></a>`distribution` | "stove0-protocol" |
| <a id="s-4f1920641f"></a>`module` | "stove0_protocol" |
| <a id="s-4de31b5966"></a>`name` | "ControllerEvidence" |
| <a id="s-2f2f608996"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [stove0_protocol.ControllerEvidence.seal](stove0-protocol-controllerevidence-seal.md)
- [stove0_protocol.ControllerEvidence.verify_digest](stove0-protocol-controllerevidence-verify-digest.md)

## Governing policies

- <a id="pa-3dce469917"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-protocol:stove0_protocol](../../../evidence/sources.md#src-084138045e) — `reference/stove0/packages/protocol/src/stove0_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_protocol.ControllerEvidence`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: ca2ff7822667383834b95e416159a1df0ec679e753097b4ee781f0d1491370c7 -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "4083962837a0d61423ed20edd5491176f060c2344623a14012265212866cd177",
    "signature": "\"(*, format: Literal['stove0-controller-evidence/v1'] = 'stove0-controller-evidence/v1', execution_envelope: stove0_protocol.models.ExecutionEnvelope, controller_evidence_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]) -> None\""
  },
  "distribution": "stove0-protocol",
  "module": "stove0_protocol",
  "name": "ControllerEvidence",
  "unit": "export"
}
```
