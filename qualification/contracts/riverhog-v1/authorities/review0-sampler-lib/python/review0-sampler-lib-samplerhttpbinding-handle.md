# review0_sampler_lib.SamplerHttpBinding.handle

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:review0-sampler-lib:review0-sampler-lib-samplerhttpbinding-handle:5efee06113 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [review0-sampler-lib](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-29ed5ae3a2"></a>
- <a id="s-371cce9540"></a>`distribution`: `review0-sampler-lib`
- <a id="s-5e1a998d10"></a>`module`: `review0_sampler_lib`
- <a id="s-f1844c342a"></a>`name`: `handle`
- <a id="s-83338345c5"></a>`owner`: `review0_sampler_lib.SamplerHttpBinding`
- <a id="s-af5bea7a92"></a>`unit`: `member`

### Declared structure

- <a id="s-69384e20e6"></a>`kind`: `"method"`
- <a id="s-a5f46ab3e3"></a>`signature`: `"\"(self, method: 'str', path: 'str', body: 'bytes' = b'') -> 'SamplerHttpResponse'\""`

## Maintained corroboration

### Related interface records

- [SamplerHttpBinding](review0-sampler-lib-samplerhttpbinding.md)

## Governing policies

- <a id="pa-4bd588f5a0"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:review0-sampler-lib:review0_sampler_lib](../../../evidence/sources/authorities.md#src-d1d6c03a07) — [some-implementations/stove0/review0/sampler/support/src/review0\_sampler\_lib/\_\_init\_\_.py](../../../../../../some-implementations/stove0/review0/sampler/support/src/review0_sampler_lib/__init__.py)

### Machine authority

- `/external_contract/python/review0_sampler_lib.SamplerHttpBinding.handle`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 2cfbea189e81773f7ff6481f2320a5f3707790dcd2c77a57e21332d1e6f12418 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, method: 'str', path: 'str', body: 'bytes' = b'') -> 'SamplerHttpResponse'\""
  },
  "distribution": "review0-sampler-lib",
  "module": "review0_sampler_lib",
  "name": "handle",
  "owner": "review0_sampler_lib.SamplerHttpBinding",
  "unit": "member"
}
```

</details>
