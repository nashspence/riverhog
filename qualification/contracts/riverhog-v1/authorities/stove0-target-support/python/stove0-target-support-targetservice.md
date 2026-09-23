# stove0_target_support.TargetService

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-support:stove0-target-support-targetservice:40bbe18b96 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-e546e745a0"></a>
- <a id="s-78b135405d"></a>`distribution`: `stove0-target-support`
- <a id="s-04c4c5ed1a"></a>`module`: `stove0_target_support`
- <a id="s-7ce3f5caeb"></a>`name`: `TargetService`
- <a id="s-568c885e8b"></a>`unit`: `export`

### Declared structure

- <a id="s-acb989a208"></a>`kind`: `"class"`
- <a id="s-3140262d88"></a>`signature`: `"'(*args, **kwargs)'"`

## Maintained corroboration

### Related interface records

- [cancel_job](stove0-target-support-targetservice-cancel-job.md)
- [contract](stove0-target-support-targetservice-contract.md)
- [get_job](stove0-target-support-targetservice-get-job.md)
- [preflight](stove0-target-support-targetservice-preflight.md)
- [put_job](stove0-target-support-targetservice-put-job.md)

## Governing policies

- <a id="pa-ac272c121b"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-target-support:stove0_target_support](../../../evidence/sources/authorities.md#src-3c01163237) — [some-implementations/stove0/packages/target-support/src/stove0\_target\_support/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/target-support/src/stove0_target_support/__init__.py)

### Machine authority

- `/external_contract/python/stove0_target_support.TargetService`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: d46fbbab54ca8bc7867963a8fb3d008ba80bbdd2d0b42bc3c0294931c91f3a2d -->

```json
{
  "contract": {
    "kind": "class",
    "signature": "'(*args, **kwargs)'"
  },
  "distribution": "stove0-target-support",
  "module": "stove0_target_support",
  "name": "TargetService",
  "unit": "export"
}
```

</details>
