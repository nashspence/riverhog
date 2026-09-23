# review0_sampler_protocol.SamplerResult.canonical_outputs

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:review0-sampler-protocol:review0-sampler-protocol-samplerresult-ca-93a715204a:5cb823f296 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [review0-sampler-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-b7fcedd27b"></a>
- <a id="s-cc41653c17"></a>`distribution`: `review0-sampler-protocol`
- <a id="s-c06dea5189"></a>`module`: `review0_sampler_protocol`
- <a id="s-5e9eb52bf6"></a>`name`: `canonical_outputs`
- <a id="s-c39520d9eb"></a>`owner`: `review0_sampler_protocol.SamplerResult`
- <a id="s-3a1bef9b43"></a>`unit`: `member`

### Declared structure

- <a id="s-2e5973ed90"></a>`kind`: `"classmethod"`
- <a id="s-1f5ce8ad75"></a>`signature`: `"\"(cls, value: 'tuple[SamplerOutput, ...]') -> 'tuple[SamplerOutput, ...]'\""`

## Maintained corroboration

### Related interface records

- [SamplerResult](review0-sampler-protocol-samplerresult.md)

## Governing policies

- <a id="pa-a530f21249"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:review0-sampler-protocol:review0_sampler_protocol](../../../evidence/sources/authorities.md#src-317a05ab5e) — [some-implementations/stove0/review0/sampler/protocol/src/review0\_sampler\_protocol/\_\_init\_\_.py](../../../../../../some-implementations/stove0/review0/sampler/protocol/src/review0_sampler_protocol/__init__.py)

### Machine authority

- `/external_contract/python/review0_sampler_protocol.SamplerResult.canonical_outputs`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 6a27b427fc2c2813cf629db5717441d3e6bcb59b86122db24bf0d852876443d4 -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, value: 'tuple[SamplerOutput, ...]') -> 'tuple[SamplerOutput, ...]'\""
  },
  "distribution": "review0-sampler-protocol",
  "module": "review0_sampler_protocol",
  "name": "canonical_outputs",
  "owner": "review0_sampler_protocol.SamplerResult",
  "unit": "member"
}
```

</details>
