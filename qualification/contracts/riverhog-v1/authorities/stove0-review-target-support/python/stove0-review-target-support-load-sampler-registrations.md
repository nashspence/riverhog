# stove0_review_target_support.load_sampler_registrations

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-review-target-support:stove0-review-target-support-load-sampler-983c3a74a6:05ff1996a2 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-review-target-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-22758ad523"></a>
- <a id="s-05f286766f"></a>`distribution`: `stove0-review-target-support`
- <a id="s-edc330e8cc"></a>`module`: `stove0_review_target_support`
- <a id="s-4b4364fc0d"></a>`name`: `load_sampler_registrations`
- <a id="s-9811133c52"></a>`unit`: `export`

### Declared structure

- <a id="s-564b8ac570"></a>`kind`: `"function"`
- <a id="s-c8ccba0bdc"></a>`signature`: `"\"(path: 'Path') -> 'tuple[SamplerRegistration, ...]'\""`

## Governing policies

- <a id="pa-922a3c2b53"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-review-target-support:stove0_review_target_support](../../../evidence/sources/authorities.md#src-2a89a71c41) — [reference/stove0/targets/review/support/src/stove0\_review\_target\_support/\_\_init\_\_.py](../../../../../../reference/stove0/targets/review/support/src/stove0_review_target_support/__init__.py)

### Machine authority

- `/external_contract/python/stove0_review_target_support.load_sampler_registrations`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 1d6056be0f8343e11dd8d280d3edc389b9a03b4b52839ba7a6f78275457d6a12 -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(path: 'Path') -> 'tuple[SamplerRegistration, ...]'\""
  },
  "distribution": "stove0-review-target-support",
  "module": "stove0_review_target_support",
  "name": "load_sampler_registrations",
  "unit": "export"
}
```

</details>
