# review0_sampler_protocol.SAMPLER_HTTP_OPERATIONS

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:review0-sampler-protocol:review0-sampler-protocol-sampler-http-operations:f4368f040a -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [review0-sampler-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-fecfb4ba4b"></a>
- <a id="s-f6349588e1"></a>`distribution`: `review0-sampler-protocol`
- <a id="s-b19cf6795c"></a>`module`: `review0_sampler_protocol`
- <a id="s-44bd1b9033"></a>`name`: `SAMPLER_HTTP_OPERATIONS`
- <a id="s-72435ed0d3"></a>`unit`: `export`

### Declared structure

- <a id="s-bb8eaa01c9"></a>`kind`: `"object"`
- <a id="s-8bc9cd5323"></a>`type`: `"builtins.tuple"`

## Governing policies

- <a id="pa-163b475183"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:review0-sampler-protocol:review0_sampler_protocol](../../../evidence/sources/authorities.md#src-317a05ab5e) — [some-implementations/stove0/review0/sampler/protocol/src/review0\_sampler\_protocol/\_\_init\_\_.py](../../../../../../some-implementations/stove0/review0/sampler/protocol/src/review0_sampler_protocol/__init__.py)

### Machine authority

- `/external_contract/python/review0_sampler_protocol.SAMPLER_HTTP_OPERATIONS`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: d77fa0365c377c53dac68e5db5606952d25ad39f2b035a576a51a0bb8ee050b7 -->

```json
{
  "contract": {
    "kind": "object",
    "type": "builtins.tuple"
  },
  "distribution": "review0-sampler-protocol",
  "module": "review0_sampler_protocol",
  "name": "SAMPLER_HTTP_OPERATIONS",
  "unit": "export"
}
```

</details>
