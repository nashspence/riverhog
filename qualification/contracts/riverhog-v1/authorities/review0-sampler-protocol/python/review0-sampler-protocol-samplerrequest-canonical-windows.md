# review0_sampler_protocol.SamplerRequest.canonical_windows

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:review0-sampler-protocol:review0-sampler-protocol-samplerrequest-c-69288dd7e8:94fe8139a7 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [review0-sampler-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-0faf32d61a"></a>
- <a id="s-592e063668"></a>`distribution`: `review0-sampler-protocol`
- <a id="s-42370043b6"></a>`module`: `review0_sampler_protocol`
- <a id="s-403a1ec1d2"></a>`name`: `canonical_windows`
- <a id="s-21eb74711b"></a>`owner`: `review0_sampler_protocol.SamplerRequest`
- <a id="s-0e10b5ca6e"></a>`unit`: `member`

### Declared structure

- <a id="s-489bf89c51"></a>`kind`: `"classmethod"`
- <a id="s-e6d7d712e8"></a>`signature`: `"\"(cls, value: 'tuple[SamplerWindow, ...]') -> 'tuple[SamplerWindow, ...]'\""`

## Maintained corroboration

### Related interface records

- [SamplerRequest](review0-sampler-protocol-samplerrequest.md)

## Governing policies

- <a id="pa-d052d07682"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:review0-sampler-protocol:review0_sampler_protocol](../../../evidence/sources/authorities.md#src-317a05ab5e) — [some-implementations/stove0/review0/sampler/protocol/src/review0\_sampler\_protocol/\_\_init\_\_.py](../../../../../../some-implementations/stove0/review0/sampler/protocol/src/review0_sampler_protocol/__init__.py)

### Machine authority

- `/external_contract/python/review0_sampler_protocol.SamplerRequest.canonical_windows`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: e3caec912a4557cadc6c36505fa68217b3f5de396cf46a86db50b55485644b19 -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, value: 'tuple[SamplerWindow, ...]') -> 'tuple[SamplerWindow, ...]'\""
  },
  "distribution": "review0-sampler-protocol",
  "module": "review0_sampler_protocol",
  "name": "canonical_windows",
  "owner": "review0_sampler_protocol.SamplerRequest",
  "unit": "member"
}
```

</details>
