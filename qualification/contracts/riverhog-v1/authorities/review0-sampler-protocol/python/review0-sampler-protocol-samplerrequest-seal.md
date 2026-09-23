# review0_sampler_protocol.SamplerRequest.seal

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:review0-sampler-protocol:review0-sampler-protocol-samplerrequest-seal:82360bacdb -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [review0-sampler-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-8f5d7a311a"></a>
- <a id="s-9cea799986"></a>`distribution`: `review0-sampler-protocol`
- <a id="s-663e6a5f0b"></a>`module`: `review0_sampler_protocol`
- <a id="s-02b056c88b"></a>`name`: `seal`
- <a id="s-4150729320"></a>`owner`: `review0_sampler_protocol.SamplerRequest`
- <a id="s-49fe502a18"></a>`unit`: `member`

### Declared structure

- <a id="s-6e58d0b4ca"></a>`kind`: `"classmethod"`
- <a id="s-3fc91d4833"></a>`signature`: `"\"(cls, payload: 'SamplerRequestPayload') -> 'SamplerRequest'\""`

## Maintained corroboration

### Related interface records

- [SamplerRequest](review0-sampler-protocol-samplerrequest.md)

## Governing policies

- <a id="pa-99d5f6413d"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:review0-sampler-protocol:review0_sampler_protocol](../../../evidence/sources/authorities.md#src-317a05ab5e) — [some-implementations/stove0/review0/sampler/protocol/src/review0\_sampler\_protocol/\_\_init\_\_.py](../../../../../../some-implementations/stove0/review0/sampler/protocol/src/review0_sampler_protocol/__init__.py)

### Machine authority

- `/external_contract/python/review0_sampler_protocol.SamplerRequest.seal`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: eda74c27ed9abc11c3b050c90c1d8fa02ac0dce3fb62bd332003b3516ba6a588 -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, payload: 'SamplerRequestPayload') -> 'SamplerRequest'\""
  },
  "distribution": "review0-sampler-protocol",
  "module": "review0_sampler_protocol",
  "name": "seal",
  "owner": "review0_sampler_protocol.SamplerRequest",
  "unit": "member"
}
```

</details>
