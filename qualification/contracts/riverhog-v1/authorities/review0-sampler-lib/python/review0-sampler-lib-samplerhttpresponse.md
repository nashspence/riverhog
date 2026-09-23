# review0_sampler_lib.SamplerHttpResponse

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:review0-sampler-lib:review0-sampler-lib-samplerhttpresponse:0b4138c8cc -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [review0-sampler-lib](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-1513f472af"></a>
- <a id="s-1a5b36c137"></a>`distribution`: `review0-sampler-lib`
- <a id="s-8f8f86451d"></a>`module`: `review0_sampler_lib`
- <a id="s-b76b0bc357"></a>`name`: `SamplerHttpResponse`
- <a id="s-8b32926d31"></a>`unit`: `export`

### Declared structure

- <a id="s-1f1a51060d"></a>`kind`: `"class"`
- <a id="s-2323e8613a"></a>`signature`: `"\"(status: 'int', headers: 'tuple[tuple[str, str], ...]', body: 'bytes') -> None\""`

#### Dataclass fields

| Field | Type | Default |
|---|---|---|
| <a id="s-39a1b8f3ff"></a>`status` | `'int'` | `required` |
| <a id="s-0c7c1496d1"></a>`headers` | `'tuple[tuple[str, str], ...]'` | `required` |
| <a id="s-09863a71ca"></a>`body` | `'bytes'` | `required` |

## Governing policies

- <a id="pa-a4e23d9ee1"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:review0-sampler-lib:review0_sampler_lib](../../../evidence/sources/authorities.md#src-d1d6c03a07) — [some-implementations/stove0/review0/sampler/support/src/review0\_sampler\_lib/\_\_init\_\_.py](../../../../../../some-implementations/stove0/review0/sampler/support/src/review0_sampler_lib/__init__.py)

### Machine authority

- `/external_contract/python/review0_sampler_lib.SamplerHttpResponse`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: acc7ccfd53d8919acc80996d82f250fc0f2c97eb50a2e7df8b6061242a1d1b5d -->

```json
{
  "contract": {
    "fields": [
      {
        "default": "required",
        "name": "status",
        "type": "'int'"
      },
      {
        "default": "required",
        "name": "headers",
        "type": "'tuple[tuple[str, str], ...]'"
      },
      {
        "default": "required",
        "name": "body",
        "type": "'bytes'"
      }
    ],
    "kind": "class",
    "signature": "\"(status: 'int', headers: 'tuple[tuple[str, str], ...]', body: 'bytes') -> None\""
  },
  "distribution": "review0-sampler-lib",
  "module": "review0_sampler_lib",
  "name": "SamplerHttpResponse",
  "unit": "export"
}
```

</details>
