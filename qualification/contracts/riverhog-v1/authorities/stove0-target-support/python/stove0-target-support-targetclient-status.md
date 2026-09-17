# stove0_target_support.TargetClient.status

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-support:stove0-target-support-targetclient-status:90e5cdf6fa -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-7ee7a94885"></a>
- <a id="s-697e1fed80"></a>`distribution`: `stove0-target-support`
- <a id="s-fb2963a60a"></a>`module`: `stove0_target_support`
- <a id="s-81b30c50d4"></a>`name`: `status`
- <a id="s-1e30e5bd4b"></a>`owner`: `stove0_target_support.TargetClient`
- <a id="s-8a6ddf0746"></a>`unit`: `member`

### Declared structure

- <a id="s-00d3035eed"></a>`kind`: `"method"`
- <a id="s-0d381317df"></a>`signature`: `"\"(self, request: 'TargetJobRequest', *, operation: 'OperationContract') -> 'TargetJobStatus'\""`

## Maintained corroboration

### Related interface records

- [TargetClient](stove0-target-support-targetclient.md)

## Governing policies

- <a id="pa-56b68823ce"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-target-support:stove0_target_support](../../../evidence/sources.md#src-3c01163237) — [reference/stove0/packages/target-support/src/stove0\_target\_support/\_\_init\_\_.py](../../../../../../reference/stove0/packages/target-support/src/stove0_target_support/__init__.py)

### Machine authority

- `/external_contract/python/stove0_target_support.TargetClient.status`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 810dd72d42d6ad4427c34c38ad3546cbb510a2a693f68f18ced92095b9db02c6 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, request: 'TargetJobRequest', *, operation: 'OperationContract') -> 'TargetJobStatus'\""
  },
  "distribution": "stove0-target-support",
  "module": "stove0_target_support",
  "name": "status",
  "owner": "stove0_target_support.TargetClient",
  "unit": "member"
}
```

</details>
