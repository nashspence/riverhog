# review0_sampler_protocol.SamplerWindow.canonical_output_path

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:review0-sampler-protocol:review0-sampler-protocol-samplerwindow-ca-03926ddb76:9371cb5897 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [review0-sampler-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-25e58780ad"></a>
- <a id="s-eb5f5166a2"></a>`distribution`: `review0-sampler-protocol`
- <a id="s-37e22ed1d7"></a>`module`: `review0_sampler_protocol`
- <a id="s-5f01721efe"></a>`name`: `canonical_output_path`
- <a id="s-c42c81ac4a"></a>`owner`: `review0_sampler_protocol.SamplerWindow`
- <a id="s-22488819ff"></a>`unit`: `member`

### Declared structure

- <a id="s-99419fb725"></a>`kind`: `"classmethod"`
- <a id="s-2ef5a07f43"></a>`signature`: `"\"(cls, value: 'str') -> 'str'\""`

## Maintained corroboration

### Related interface records

- [SamplerWindow](review0-sampler-protocol-samplerwindow.md)

## Governing policies

- <a id="pa-0a4089f24f"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:review0-sampler-protocol:review0_sampler_protocol](../../../evidence/sources/authorities.md#src-317a05ab5e) — [some-implementations/stove0/review0/sampler/protocol/src/review0\_sampler\_protocol/\_\_init\_\_.py](../../../../../../some-implementations/stove0/review0/sampler/protocol/src/review0_sampler_protocol/__init__.py)

### Machine authority

- `/external_contract/python/review0_sampler_protocol.SamplerWindow.canonical_output_path`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: eab2eb69722169269f93167725d426c32667a25cb26c68cebab31a99e932c992 -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, value: 'str') -> 'str'\""
  },
  "distribution": "review0-sampler-protocol",
  "module": "review0_sampler_protocol",
  "name": "canonical_output_path",
  "owner": "review0_sampler_protocol.SamplerWindow",
  "unit": "member"
}
```

</details>
