# stove0_target_protocol.TargetContract.verify_digest

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-protocol:stove0-target-protocol-targetcontract-verify-digest:d65d4b2cd1 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-f78a0d78a3"></a>
- <a id="s-d670b8a885"></a>`distribution`: `stove0-target-protocol`
- <a id="s-579d1fde0f"></a>`module`: `stove0_target_protocol`
- <a id="s-b66a6b30e4"></a>`name`: `verify_digest`
- <a id="s-30e393b74c"></a>`owner`: `stove0_target_protocol.TargetContract`
- <a id="s-cda01252a0"></a>`unit`: `member`

### Declared structure

- <a id="s-6674dc0851"></a>`kind`: `"method"`
- <a id="s-3d36cf39d7"></a>`signature`: `"\"(self) -> 'Self'\""`

## Maintained corroboration

### Related interface records

- [stove0_target_protocol.TargetContract](stove0-target-protocol-targetcontract.md)

## Governing policies

- <a id="pa-16ec403bd8"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-target-protocol:stove0_target_protocol](../../../evidence/sources.md#src-f4f0b22026) — `reference/stove0/packages/target-protocol/src/stove0_target_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_target_protocol.TargetContract.verify_digest`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 90a48703619ad43305e91bcdab881b3057d4b3b770532457e3f58be94b14a25b -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'Self'\""
  },
  "distribution": "stove0-target-protocol",
  "module": "stove0_target_protocol",
  "name": "verify_digest",
  "owner": "stove0_target_protocol.TargetContract",
  "unit": "member"
}
```
