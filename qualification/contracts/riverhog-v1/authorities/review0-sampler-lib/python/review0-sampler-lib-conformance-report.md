# review0_sampler_lib.conformance_report

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:review0-sampler-lib:review0-sampler-lib-conformance-report:9e608ebaf5 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [review0-sampler-lib](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-43a6e23d34"></a>
- <a id="s-24eba0da7d"></a>`distribution`: `review0-sampler-lib`
- <a id="s-3435f81406"></a>`module`: `review0_sampler_lib`
- <a id="s-a0b6561475"></a>`name`: `conformance_report`
- <a id="s-ab6e840619"></a>`unit`: `export`

### Declared structure

- <a id="s-701829a62e"></a>`kind`: `"function"`
- <a id="s-f244886b61"></a>`signature`: `"\"(client: 'SamplerClient', *, request: 'SamplerRequest \| None' = None) -> 'SamplerConformanceResult'\""`

## Governing policies

- <a id="pa-44aa1b5a12"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:review0-sampler-lib:review0_sampler_lib](../../../evidence/sources/authorities.md#src-d1d6c03a07) — [some-implementations/stove0/review0/sampler/support/src/review0\_sampler\_lib/\_\_init\_\_.py](../../../../../../some-implementations/stove0/review0/sampler/support/src/review0_sampler_lib/__init__.py)

### Machine authority

- `/external_contract/python/review0_sampler_lib.conformance_report`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 6f5153337547eae582d199f046da627f13a2f8b3cf9bb89e27a6458900bfd736 -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(client: 'SamplerClient', *, request: 'SamplerRequest | None' = None) -> 'SamplerConformanceResult'\""
  },
  "distribution": "review0-sampler-lib",
  "module": "review0_sampler_lib",
  "name": "conformance_report",
  "unit": "export"
}
```

</details>
