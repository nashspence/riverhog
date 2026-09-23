# review0_sampler_protocol.SamplerRequestPayload.canonical_windows

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:review0-sampler-protocol:review0-sampler-protocol-samplerrequestpa-c7a1987e51:681d38e064 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [review0-sampler-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-f848ca67fd"></a>
- <a id="s-1b304dca79"></a>`distribution`: `review0-sampler-protocol`
- <a id="s-b211b0460d"></a>`module`: `review0_sampler_protocol`
- <a id="s-f4400adee8"></a>`name`: `canonical_windows`
- <a id="s-3be187d93a"></a>`owner`: `review0_sampler_protocol.SamplerRequestPayload`
- <a id="s-15b0962e6d"></a>`unit`: `member`

### Declared structure

- <a id="s-9cce7d0a69"></a>`kind`: `"classmethod"`
- <a id="s-8c48437410"></a>`signature`: `"\"(cls, value: 'tuple[SamplerWindow, ...]') -> 'tuple[SamplerWindow, ...]'\""`

## Maintained corroboration

### Related interface records

- [SamplerRequestPayload](review0-sampler-protocol-samplerrequestpayload.md)

## Governing policies

- <a id="pa-a3e9d0f987"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:review0-sampler-protocol:review0_sampler_protocol](../../../evidence/sources/authorities.md#src-317a05ab5e) — [some-implementations/stove0/review0/sampler/protocol/src/review0\_sampler\_protocol/\_\_init\_\_.py](../../../../../../some-implementations/stove0/review0/sampler/protocol/src/review0_sampler_protocol/__init__.py)

### Machine authority

- `/external_contract/python/review0_sampler_protocol.SamplerRequestPayload.canonical_windows`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 485d3e12414b9ebe1c770a09cc7aae463043b35baafa74958962f132fedcddd7 -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, value: 'tuple[SamplerWindow, ...]') -> 'tuple[SamplerWindow, ...]'\""
  },
  "distribution": "review0-sampler-protocol",
  "module": "review0_sampler_protocol",
  "name": "canonical_windows",
  "owner": "review0_sampler_protocol.SamplerRequestPayload",
  "unit": "member"
}
```

</details>
