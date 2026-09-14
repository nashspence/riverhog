# stove0_target_support.TargetClient

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-support:stove0-target-support-targetclient:6fa7df9f1e -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-a34e3fa0d8"></a>
- <a id="s-1a7f9ab488"></a>`distribution`: `stove0-target-support`
- <a id="s-23b80fd256"></a>`module`: `stove0_target_support`
- <a id="s-11f9a12ef0"></a>`name`: `TargetClient`
- <a id="s-10b5319ce9"></a>`unit`: `export`

### Declared structure

- <a id="s-0c7a0d6867"></a>`kind`: `"class"`
- <a id="s-59863b13f0"></a>`signature`: `"'(*args, **kwargs)'"`

## Maintained corroboration

### Related interface records

- [stove0_target_support.TargetClient.contract](stove0-target-support-targetclient-contract.md)
- [stove0_target_support.TargetClient.preflight](stove0-target-support-targetclient-preflight.md)
- [stove0_target_support.TargetClient.put_job](stove0-target-support-targetclient-put-job.md)
- [stove0_target_support.TargetClient.status](stove0-target-support-targetclient-status.md)

## Governing policies

- <a id="pa-45a02e18bf"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-target-support:stove0_target_support](../../../evidence/sources.md#src-3c01163237) — `reference/stove0/packages/target-support/src/stove0_target_support/__init__.py`

### Machine authority

- `/external_contract/python/stove0_target_support.TargetClient`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 03f55c064b6a3e122cc9384df4e28bccad72a8d95094e8ed77c3c93df280b0a5 -->

```json
{
  "contract": {
    "kind": "class",
    "signature": "'(*args, **kwargs)'"
  },
  "distribution": "stove0-target-support",
  "module": "stove0_target_support",
  "name": "TargetClient",
  "unit": "export"
}
```
