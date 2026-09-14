# stove0_target_protocol.TargetPreflightRequest.canonical_observations

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-protocol:stove0-target-protocol-targetpreflightreq-546266f5e3:3a9fbc89ca -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-8247bd344b"></a>
- <a id="s-9fa5b4e219"></a>`distribution`: `stove0-target-protocol`
- <a id="s-1efaf930c5"></a>`module`: `stove0_target_protocol`
- <a id="s-57c88d1fcf"></a>`name`: `canonical_observations`
- <a id="s-d2fe4ef08f"></a>`owner`: `stove0_target_protocol.TargetPreflightRequest`
- <a id="s-1d618e541c"></a>`unit`: `member`

### Declared structure

- <a id="s-5c0a598120"></a>`kind`: `"classmethod"`
- <a id="s-27ba1345f5"></a>`signature`: `"\"(cls, value: 'tuple[ObservationEvidence, ...]') -> 'tuple[ObservationEvidence, ...]'\""`

## Maintained corroboration

### Related interface records

- [TargetPreflightRequest](stove0-target-protocol-targetpreflightrequest.md)

## Governing policies

- <a id="pa-6c002cae19"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-target-protocol:stove0_target_protocol](../../../evidence/sources.md#src-f4f0b22026) — `reference/stove0/packages/target-protocol/src/stove0_target_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_target_protocol.TargetPreflightRequest.canonical_observations`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: acc8adb8853eb56e270f575d005c6d1db218d6a0a2770862f693b65df31f3de1 -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, value: 'tuple[ObservationEvidence, ...]') -> 'tuple[ObservationEvidence, ...]'\""
  },
  "distribution": "stove0-target-protocol",
  "module": "stove0_target_protocol",
  "name": "canonical_observations",
  "owner": "stove0_target_protocol.TargetPreflightRequest",
  "unit": "member"
}
```
