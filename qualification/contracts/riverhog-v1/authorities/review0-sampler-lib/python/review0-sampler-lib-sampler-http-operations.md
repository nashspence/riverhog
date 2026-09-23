# review0_sampler_lib.SAMPLER_HTTP_OPERATIONS

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:review0-sampler-lib:review0-sampler-lib-sampler-http-operations:12ead070ad -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [review0-sampler-lib](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-291070c1d0"></a>
- <a id="s-785b7b8941"></a>`distribution`: `review0-sampler-lib`
- <a id="s-0824ad39da"></a>`module`: `review0_sampler_lib`
- <a id="s-59f371a38e"></a>`name`: `SAMPLER_HTTP_OPERATIONS`
- <a id="s-76c50ecb46"></a>`unit`: `export`

### Declared structure

- <a id="s-43e7c14758"></a>`kind`: `"object"`
- <a id="s-f3a4fa30a9"></a>`type`: `"builtins.tuple"`

## Governing policies

- <a id="pa-112a3ebf31"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:review0-sampler-lib:review0_sampler_lib](../../../evidence/sources/authorities.md#src-d1d6c03a07) — [some-implementations/stove0/review0/sampler/support/src/review0\_sampler\_lib/\_\_init\_\_.py](../../../../../../some-implementations/stove0/review0/sampler/support/src/review0_sampler_lib/__init__.py)

### Machine authority

- `/external_contract/python/review0_sampler_lib.SAMPLER_HTTP_OPERATIONS`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: bf57dd2530efdf82186dc548fb9d770f31716b0442238aa57131ae1bb09ce7ad -->

```json
{
  "contract": {
    "kind": "object",
    "type": "builtins.tuple"
  },
  "distribution": "review0-sampler-lib",
  "module": "review0_sampler_lib",
  "name": "SAMPLER_HTTP_OPERATIONS",
  "unit": "export"
}
```

</details>
