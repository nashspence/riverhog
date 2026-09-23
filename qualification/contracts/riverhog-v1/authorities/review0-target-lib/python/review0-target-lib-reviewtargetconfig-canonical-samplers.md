# review0_target_lib.ReviewTargetConfig.canonical_samplers

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:review0-target-lib:review0-target-lib-reviewtargetconfig-can-5a71f5cae3:150a9c85cc -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [review0-target-lib](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-d096436c9d"></a>
- <a id="s-6e2d742491"></a>`distribution`: `review0-target-lib`
- <a id="s-910ae53c5a"></a>`module`: `review0_target_lib`
- <a id="s-c1a7413e44"></a>`name`: `canonical_samplers`
- <a id="s-92b13a23e1"></a>`owner`: `review0_target_lib.ReviewTargetConfig`
- <a id="s-9d8d6c14c9"></a>`unit`: `member`

### Declared structure

- <a id="s-be80597e58"></a>`kind`: `"classmethod"`
- <a id="s-cf3c3c9cd0"></a>`signature`: `"\"(cls, value: 'tuple[SamplerConfig, ...]') -> 'tuple[SamplerConfig, ...]'\""`

## Maintained corroboration

### Related interface records

- [ReviewTargetConfig](review0-target-lib-reviewtargetconfig.md)

## Governing policies

- <a id="pa-fb3f1bf20d"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:review0-target-lib:review0_target_lib](../../../evidence/sources/authorities.md#src-665023c8f6) — [some-implementations/stove0/review0/support/src/review0\_target\_lib/\_\_init\_\_.py](../../../../../../some-implementations/stove0/review0/support/src/review0_target_lib/__init__.py)

### Machine authority

- `/external_contract/python/review0_target_lib.ReviewTargetConfig.canonical_samplers`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: fb0b71faea05b3e0cea7e20385c51c589e93b7a154fa632efe739d368dc3a069 -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, value: 'tuple[SamplerConfig, ...]') -> 'tuple[SamplerConfig, ...]'\""
  },
  "distribution": "review0-target-lib",
  "module": "review0_target_lib",
  "name": "canonical_samplers",
  "owner": "review0_target_lib.ReviewTargetConfig",
  "unit": "member"
}
```

</details>
