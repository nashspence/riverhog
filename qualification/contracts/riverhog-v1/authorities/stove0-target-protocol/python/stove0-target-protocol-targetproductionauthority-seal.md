# stove0_target_protocol.TargetProductionAuthority.seal

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-protocol:stove0-target-protocol-targetproductionau-0afcc88eb3:eb072688ea -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-0264045b5e"></a>
- <a id="s-0c7ef3b74e"></a>`distribution`: `stove0-target-protocol`
- <a id="s-3fb209dcea"></a>`module`: `stove0_target_protocol`
- <a id="s-cd3ce4ebf0"></a>`name`: `seal`
- <a id="s-2c702075da"></a>`owner`: `stove0_target_protocol.TargetProductionAuthority`
- <a id="s-1614dd736c"></a>`unit`: `member`

### Declared structure

- <a id="s-19f1fe4cd4"></a>`kind`: `"classmethod"`
- <a id="s-60fc885171"></a>`signature`: `"\"(cls, payload: 'TargetProductionAuthorityPayload') -> 'TargetProductionAuthority'\""`

## Maintained corroboration

### Related interface records

- [TargetProductionAuthority](stove0-target-protocol-targetproductionauthority.md)

## Governing policies

- <a id="pa-857b91ced5"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-target-protocol:stove0_target_protocol](../../../evidence/sources.md#src-f4f0b22026) — [reference/stove0/packages/target-protocol/src/stove0\_target\_protocol/\_\_init\_\_.py](../../../../../../reference/stove0/packages/target-protocol/src/stove0_target_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_target_protocol.TargetProductionAuthority.seal`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 163d7d2a2096a3618b44365cccfa4bcaa20cd9e1cc90c550c3267dc8db44d602 -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, payload: 'TargetProductionAuthorityPayload') -> 'TargetProductionAuthority'\""
  },
  "distribution": "stove0-target-protocol",
  "module": "stove0_target_protocol",
  "name": "seal",
  "owner": "stove0_target_protocol.TargetProductionAuthority",
  "unit": "member"
}
```

</details>
