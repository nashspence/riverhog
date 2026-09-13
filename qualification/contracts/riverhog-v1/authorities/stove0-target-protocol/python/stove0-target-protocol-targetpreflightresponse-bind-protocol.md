# stove0_target_protocol.TargetPreflightResponse.bind_protocol

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-protocol:stove0-target-protocol-targetpreflightres-b2dc0dd912:fda73efb6f -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-aa7069f804"></a>
| Field | Shape |
|---|---|
| <a id="s-133068b1f5"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-7f5e3bd631"></a>`distribution` | "stove0-target-protocol" |
| <a id="s-fb66ebce17"></a>`module` | "stove0_target_protocol" |
| <a id="s-bb930a4d55"></a>`name` | "bind_protocol" |
| <a id="s-ef576e1f22"></a>`owner` | "stove0_target_protocol.TargetPreflightResponse" |
| <a id="s-ad588380b6"></a>`unit` | "member" |

## Maintained corroboration

### Related interface records

- [stove0_target_protocol.TargetPreflightResponse](stove0-target-protocol-targetpreflightresponse.md)

## Governing policies

- <a id="pa-2f4dc67623"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-target-protocol:stove0_target_protocol](../../../evidence/sources.md#src-f4f0b22026) — `reference/stove0/packages/target-protocol/src/stove0_target_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_target_protocol.TargetPreflightResponse.bind_protocol`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 1b802d01a640fe218a3d502528a9db86ec9f12650efd45730b48cadc8a4fb315 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'Self'\""
  },
  "distribution": "stove0-target-protocol",
  "module": "stove0_target_protocol",
  "name": "bind_protocol",
  "owner": "stove0_target_protocol.TargetPreflightResponse",
  "unit": "member"
}
```
