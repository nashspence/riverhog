# stove0_review_sampler_support.SamplerHttpResponse

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-review-sampler-support:stove0-review-sampler-support-samplerhttpresponse:04427ebebc -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-review-sampler-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-6c5e53fb63"></a>
- <a id="s-de9577eb57"></a>`distribution`: `stove0-review-sampler-support`
- <a id="s-b90102e922"></a>`module`: `stove0_review_sampler_support`
- <a id="s-900f3b3859"></a>`name`: `SamplerHttpResponse`
- <a id="s-59b1ba9202"></a>`unit`: `export`

### Declared structure

- <a id="s-21fcdfb0b3"></a>`kind`: `"class"`
- <a id="s-486af26366"></a>`signature`: `"\"(status: 'int', headers: 'tuple[tuple[str, str], ...]', body: 'bytes') -> None\""`

#### Dataclass fields

| Field | Type | Default |
|---|---|---|
| <a id="s-b96fe9dc5b"></a>`status` | `'int'` | `required` |
| <a id="s-be18c7eefa"></a>`headers` | `'tuple[tuple[str, str], ...]'` | `required` |
| <a id="s-e321aaaea2"></a>`body` | `'bytes'` | `required` |

## Governing policies

- <a id="pa-d68ee30b36"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-review-sampler-support:stove0_review_sampler_support](../../../evidence/sources/authorities.md#src-6dd798b0df) — [reference/stove0/targets/review/sampler/support/src/stove0\_review\_sampler\_support/\_\_init\_\_.py](../../../../../../reference/stove0/targets/review/sampler/support/src/stove0_review_sampler_support/__init__.py)

### Machine authority

- `/external_contract/python/stove0_review_sampler_support.SamplerHttpResponse`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 995a25b6fb964028d491fb27c7706c3fce7e16b37c6df98f9543d7b545781e34 -->

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
  "distribution": "stove0-review-sampler-support",
  "module": "stove0_review_sampler_support",
  "name": "SamplerHttpResponse",
  "unit": "export"
}
```

</details>
