# stove0_target_support.TargetClient.preflight

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-support:stove0-target-support-targetclient-preflight:7d6f76dc79 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-0e71ee85cc"></a>
- <a id="s-64ecd25006"></a>`distribution`: `stove0-target-support`
- <a id="s-93fcac4608"></a>`module`: `stove0_target_support`
- <a id="s-0b87adea66"></a>`name`: `preflight`
- <a id="s-bf91a9e246"></a>`owner`: `stove0_target_support.TargetClient`
- <a id="s-6c3f837bd6"></a>`unit`: `member`

### Declared structure

- <a id="s-855c988016"></a>`kind`: `"method"`
- <a id="s-2b4f0361a3"></a>`signature`: `"\"(self, request: 'TargetPreflightRequest') -> 'TargetPreflightResponse'\""`

## Maintained corroboration

### Related interface records

- [stove0_target_support.TargetClient](stove0-target-support-targetclient.md)

## Governing policies

- <a id="pa-416a7e436a"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-target-support:stove0_target_support](../../../evidence/sources.md#src-3c01163237) — `reference/stove0/packages/target-support/src/stove0_target_support/__init__.py`

### Machine authority

- `/external_contract/python/stove0_target_support.TargetClient.preflight`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 172e610a01199c75b97a3034b48b51c0a2aece4925b99b54b081eee10b1bbca1 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, request: 'TargetPreflightRequest') -> 'TargetPreflightResponse'\""
  },
  "distribution": "stove0-target-support",
  "module": "stove0_target_support",
  "name": "preflight",
  "owner": "stove0_target_support.TargetClient",
  "unit": "member"
}
```
