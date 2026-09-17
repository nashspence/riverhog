# stove0_protocol.JoinPlan.verify_contract

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-protocol:stove0-protocol-joinplan-verify-contract:525a64a584 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-c4c4870924"></a>
- <a id="s-e60bb81f92"></a>`distribution`: `stove0-protocol`
- <a id="s-a005af3e27"></a>`module`: `stove0_protocol`
- <a id="s-44b1a3f67c"></a>`name`: `verify_contract`
- <a id="s-2fa6513cb2"></a>`owner`: `stove0_protocol.JoinPlan`
- <a id="s-aefc6937e6"></a>`unit`: `member`

### Declared structure

- <a id="s-192e4f9e9a"></a>`kind`: `"method"`
- <a id="s-f578226b8f"></a>`signature`: `"\"(self) -> 'Self'\""`

## Maintained corroboration

### Related interface records

- [JoinPlan](stove0-protocol-joinplan.md)

## Governing policies

- <a id="pa-735fafc047"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-protocol:stove0_protocol](../../../evidence/sources.md#src-084138045e) — [reference/stove0/packages/protocol/src/stove0\_protocol/\_\_init\_\_.py](../../../../../../reference/stove0/packages/protocol/src/stove0_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_protocol.JoinPlan.verify_contract`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 97352ea83a66892c713f01ac4f4399325dc9fd30637b0d61bf8781ed85d53077 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'Self'\""
  },
  "distribution": "stove0-protocol",
  "module": "stove0_protocol",
  "name": "verify_contract",
  "owner": "stove0_protocol.JoinPlan",
  "unit": "member"
}
```

</details>
