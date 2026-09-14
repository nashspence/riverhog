# stove0_target_protocol.TargetJobRequest.accepted

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-protocol:stove0-target-protocol-targetjobrequest-accepted:4775d3e3a3 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-53ea3b0955"></a>
- <a id="s-6182d1fce7"></a>`distribution`: `stove0-target-protocol`
- <a id="s-148464206d"></a>`module`: `stove0_target_protocol`
- <a id="s-1e22783419"></a>`name`: `accepted`
- <a id="s-4fe2ef03fe"></a>`owner`: `stove0_target_protocol.TargetJobRequest`
- <a id="s-f4bbac4255"></a>`unit`: `member`

### Declared structure

- <a id="s-a67b0fb687"></a>`kind`: `"method"`
- <a id="s-c261a7e29b"></a>`signature`: `"\"(self) -> 'AcceptedTargetJob'\""`

## Maintained corroboration

### Related interface records

- [TargetJobRequest](stove0-target-protocol-targetjobrequest.md)

## Governing policies

- <a id="pa-370c8842b8"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-target-protocol:stove0_target_protocol](../../../evidence/sources.md#src-f4f0b22026) — `reference/stove0/packages/target-protocol/src/stove0_target_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_target_protocol.TargetJobRequest.accepted`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: e9f9379df7b73100d4a261b6dbc08b1a2d2ffa441fc80a9344231da385aabdef -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'AcceptedTargetJob'\""
  },
  "distribution": "stove0-target-protocol",
  "module": "stove0_target_protocol",
  "name": "accepted",
  "owner": "stove0_target_protocol.TargetJobRequest",
  "unit": "member"
}
```
