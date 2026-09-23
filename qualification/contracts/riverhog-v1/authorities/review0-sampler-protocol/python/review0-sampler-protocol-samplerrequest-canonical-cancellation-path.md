# review0_sampler_protocol.SamplerRequest.canonical_cancellation_path

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:review0-sampler-protocol:review0-sampler-protocol-samplerrequest-c-6b2b92b1f6:b0cd816af1 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [review0-sampler-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-fa61ffe58a"></a>
- <a id="s-9b3fb712d6"></a>`distribution`: `review0-sampler-protocol`
- <a id="s-72b7b42897"></a>`module`: `review0_sampler_protocol`
- <a id="s-26195cd8c1"></a>`name`: `canonical_cancellation_path`
- <a id="s-7c37d9df3d"></a>`owner`: `review0_sampler_protocol.SamplerRequest`
- <a id="s-1cbab408d8"></a>`unit`: `member`

### Declared structure

- <a id="s-e72e2a567b"></a>`kind`: `"classmethod"`
- <a id="s-0cf5f14b2d"></a>`signature`: `"\"(cls, value: 'str') -> 'str'\""`

## Maintained corroboration

### Related interface records

- [SamplerRequest](review0-sampler-protocol-samplerrequest.md)

## Governing policies

- <a id="pa-ae94438aef"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:review0-sampler-protocol:review0_sampler_protocol](../../../evidence/sources/authorities.md#src-317a05ab5e) — [some-implementations/stove0/review0/sampler/protocol/src/review0\_sampler\_protocol/\_\_init\_\_.py](../../../../../../some-implementations/stove0/review0/sampler/protocol/src/review0_sampler_protocol/__init__.py)

### Machine authority

- `/external_contract/python/review0_sampler_protocol.SamplerRequest.canonical_cancellation_path`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 90f9ea048b17b2c7ebbbfae75299d3c9390cfe4ec7fb5796e5a09c5c8776d9ee -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, value: 'str') -> 'str'\""
  },
  "distribution": "review0-sampler-protocol",
  "module": "review0_sampler_protocol",
  "name": "canonical_cancellation_path",
  "owner": "review0_sampler_protocol.SamplerRequest",
  "unit": "member"
}
```

</details>
