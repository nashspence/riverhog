# stove0_observer_protocol.ObservationEvidence

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-observer-protocol:stove0-observer-protocol-observationevidence:ea40311ee4 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-observer-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-92a28a28e1"></a>
| Field | Shape |
|---|---|
| <a id="s-e71f69524f"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-7352ffa93b"></a>`distribution` | "stove0-observer-protocol" |
| <a id="s-fad3bbfd0e"></a>`module` | "stove0_observer_protocol" |
| <a id="s-f154ce5c29"></a>`name` | "ObservationEvidence" |
| <a id="s-3eb8202b3e"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [stove0_observer_protocol.ObservationEvidence.bind_result](stove0-observer-protocol-observationevidence-bind-result.md)

## Governing policies

- <a id="pa-4805da7b58"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-observer-protocol:stove0_observer_protocol](../../../evidence/sources.md#src-62450e0156) — `reference/stove0/packages/observer-protocol/src/stove0_observer_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_observer_protocol.ObservationEvidence`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 579f62dc1ac5f974192db50f76f357dbf41d0fa7147956d85e1739073a3aa421 -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "fe203426a6a155925058ab72a4cdf1c5ed682d85bae5b822f1a1f67bfee09fc9",
    "signature": "'(*, request: stove0_protocol.models.ObservationRequest, result: stove0_protocol.models.ObservationResult) -> None'"
  },
  "distribution": "stove0-observer-protocol",
  "module": "stove0_observer_protocol",
  "name": "ObservationEvidence",
  "unit": "export"
}
```
