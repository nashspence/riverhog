# stove0_target_protocol.EffectPlan.verify_digest

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-protocol:stove0-target-protocol-effectplan-verify-digest:396350212d -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-42e624988c"></a>
- <a id="s-e4e57b965e"></a>`distribution`: `stove0-target-protocol`
- <a id="s-afd1c3f631"></a>`module`: `stove0_target_protocol`
- <a id="s-75be6e1440"></a>`name`: `verify_digest`
- <a id="s-32584bc636"></a>`owner`: `stove0_target_protocol.EffectPlan`
- <a id="s-f801195fcc"></a>`unit`: `member`

### Declared structure

- <a id="s-093bab220b"></a>`kind`: `"method"`
- <a id="s-43d8bd2970"></a>`signature`: `"\"(self) -> 'Self'\""`

## Maintained corroboration

### Related interface records

- [EffectPlan](stove0-target-protocol-effectplan.md)

## Governing policies

- <a id="pa-240ef8957a"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-target-protocol:stove0_target_protocol](../../../evidence/sources/authorities.md#src-f4f0b22026) — [some-implementations/stove0/packages/target-protocol/src/stove0\_target\_protocol/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/target-protocol/src/stove0_target_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_target_protocol.EffectPlan.verify_digest`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 6b906814ac2f8f01a97ad9ab81ac3c905d67a95633d0b6089e6aae65c37dd761 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'Self'\""
  },
  "distribution": "stove0-target-protocol",
  "module": "stove0_target_protocol",
  "name": "verify_digest",
  "owner": "stove0_target_protocol.EffectPlan",
  "unit": "member"
}
```

</details>
