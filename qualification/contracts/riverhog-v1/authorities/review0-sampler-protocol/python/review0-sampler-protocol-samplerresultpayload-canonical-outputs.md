# review0_sampler_protocol.SamplerResultPayload.canonical_outputs

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:review0-sampler-protocol:review0-sampler-protocol-samplerresultpay-af4529772d:55fc42c2f9 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [review0-sampler-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-ab86271a7d"></a>
- <a id="s-1de544a1d2"></a>`distribution`: `review0-sampler-protocol`
- <a id="s-7aabdd5f31"></a>`module`: `review0_sampler_protocol`
- <a id="s-a8b881d7b4"></a>`name`: `canonical_outputs`
- <a id="s-4b1302e3a3"></a>`owner`: `review0_sampler_protocol.SamplerResultPayload`
- <a id="s-2438cab7de"></a>`unit`: `member`

### Declared structure

- <a id="s-27d3a17e8f"></a>`kind`: `"classmethod"`
- <a id="s-be42f33fb4"></a>`signature`: `"\"(cls, value: 'tuple[SamplerOutput, ...]') -> 'tuple[SamplerOutput, ...]'\""`

## Maintained corroboration

### Related interface records

- [SamplerResultPayload](review0-sampler-protocol-samplerresultpayload.md)

## Governing policies

- <a id="pa-6090468017"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:review0-sampler-protocol:review0_sampler_protocol](../../../evidence/sources/authorities.md#src-317a05ab5e) — [some-implementations/stove0/review0/sampler/protocol/src/review0\_sampler\_protocol/\_\_init\_\_.py](../../../../../../some-implementations/stove0/review0/sampler/protocol/src/review0_sampler_protocol/__init__.py)

### Machine authority

- `/external_contract/python/review0_sampler_protocol.SamplerResultPayload.canonical_outputs`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 29355588131cdfb09c33cc8df60afe4acd608c8676099ec647d82b3348d7dcda -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, value: 'tuple[SamplerOutput, ...]') -> 'tuple[SamplerOutput, ...]'\""
  },
  "distribution": "review0-sampler-protocol",
  "module": "review0_sampler_protocol",
  "name": "canonical_outputs",
  "owner": "review0_sampler_protocol.SamplerResultPayload",
  "unit": "member"
}
```

</details>
