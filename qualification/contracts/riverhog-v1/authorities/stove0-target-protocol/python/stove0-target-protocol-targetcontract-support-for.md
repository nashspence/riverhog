# stove0_target_protocol.TargetContract.support_for

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-protocol:stove0-target-protocol-targetcontract-support-for:b6bedac32f -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-674a97d618"></a>
- <a id="s-05752a7906"></a>`distribution`: `stove0-target-protocol`
- <a id="s-f33b06ca40"></a>`module`: `stove0_target_protocol`
- <a id="s-8c21bde371"></a>`name`: `support_for`
- <a id="s-2caee7806e"></a>`owner`: `stove0_target_protocol.TargetContract`
- <a id="s-7fedcd42c6"></a>`unit`: `member`

### Declared structure

- <a id="s-c3fa030e17"></a>`kind`: `"method"`
- <a id="s-03fc1d86c0"></a>`signature`: `"\"(self, operation_id: 'str') -> 'TargetOperationSupport'\""`

## Maintained corroboration

### Related interface records

- [TargetContract](stove0-target-protocol-targetcontract.md)

## Governing policies

- <a id="pa-0576426e69"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-target-protocol:stove0_target_protocol](../../../evidence/sources.md#src-f4f0b22026) — `reference/stove0/packages/target-protocol/src/stove0_target_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_target_protocol.TargetContract.support_for`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: c53dd9578fc77e4fbee7fa641cb93e989a6f13f8f67549a59eba11ebecdf8b4f -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, operation_id: 'str') -> 'TargetOperationSupport'\""
  },
  "distribution": "stove0-target-protocol",
  "module": "stove0_target_protocol",
  "name": "support_for",
  "owner": "stove0_target_protocol.TargetContract",
  "unit": "member"
}
```

</details>
