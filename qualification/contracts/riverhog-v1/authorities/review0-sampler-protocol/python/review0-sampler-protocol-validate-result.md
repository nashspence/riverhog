# review0_sampler_protocol.validate_result

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:review0-sampler-protocol:review0-sampler-protocol-validate-result:4aac211a29 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [review0-sampler-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-3a50deb180"></a>
- <a id="s-4fc6a1c31e"></a>`distribution`: `review0-sampler-protocol`
- <a id="s-427a802a0c"></a>`module`: `review0_sampler_protocol`
- <a id="s-d89e2be495"></a>`name`: `validate_result`
- <a id="s-864fcca460"></a>`unit`: `export`

### Declared structure

- <a id="s-9d58b967a0"></a>`kind`: `"function"`
- <a id="s-2a088b1446"></a>`signature`: `"\"(result: 'SamplerResult', request: 'SamplerRequest', descriptor: 'SamplerDescriptor') -> 'None'\""`

## Governing policies

- <a id="pa-565a3659ea"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:review0-sampler-protocol:review0_sampler_protocol](../../../evidence/sources/authorities.md#src-317a05ab5e) — [some-implementations/stove0/review0/sampler/protocol/src/review0\_sampler\_protocol/\_\_init\_\_.py](../../../../../../some-implementations/stove0/review0/sampler/protocol/src/review0_sampler_protocol/__init__.py)

### Machine authority

- `/external_contract/python/review0_sampler_protocol.validate_result`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 90156e0daeb2328f9b7ca25a133bb1dafff9570f121397a762c862c22e961822 -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(result: 'SamplerResult', request: 'SamplerRequest', descriptor: 'SamplerDescriptor') -> 'None'\""
  },
  "distribution": "review0-sampler-protocol",
  "module": "review0_sampler_protocol",
  "name": "validate_result",
  "unit": "export"
}
```

</details>
