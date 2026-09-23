# review0_sampler_client.SamplerProtocolError

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:review0-sampler-client:review0-sampler-client-samplerprotocolerror:d93860cf33 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [review0-sampler-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-29f9eb28f0"></a>
- <a id="s-92a24c8786"></a>`distribution`: `review0-sampler-client`
- <a id="s-08a4a77274"></a>`module`: `review0_sampler_client`
- <a id="s-8a53092dbc"></a>`name`: `SamplerProtocolError`
- <a id="s-9da65355af"></a>`unit`: `export`

### Declared structure

- <a id="s-44a63850f1"></a>`kind`: `"class"`
- <a id="s-7ff3eafaa1"></a>`signature`: `"'(message: \\'str\\', *, failure_kind: \"Literal[\\'remote_rejection\\', \\'transport\\', \\'invalid_response\\']\", code: \\'str \| None\\' = None, observed_status: \\'int \| None\\' = None, details: \\'Mapping[str, Any] \| None\\' = None) -> \\'None\\''"`

## Governing policies

- <a id="pa-b72a7978f6"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:review0-sampler-client:review0_sampler_client](../../../evidence/sources/authorities.md#src-eb22a957aa) — [some-implementations/stove0/review0/sampler/client/src/review0\_sampler\_client/\_\_init\_\_.py](../../../../../../some-implementations/stove0/review0/sampler/client/src/review0_sampler_client/__init__.py)

### Machine authority

- `/external_contract/python/review0_sampler_client.SamplerProtocolError`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: a4f09fa05cfa35ffae6ca02120343c62d9b33db036c8b8a78e98a51467ad4a46 -->

```json
{
  "contract": {
    "kind": "class",
    "signature": "'(message: \\'str\\', *, failure_kind: \"Literal[\\'remote_rejection\\', \\'transport\\', \\'invalid_response\\']\", code: \\'str | None\\' = None, observed_status: \\'int | None\\' = None, details: \\'Mapping[str, Any] | None\\' = None) -> \\'None\\''"
  },
  "distribution": "review0-sampler-client",
  "module": "review0_sampler_client",
  "name": "SamplerProtocolError",
  "unit": "export"
}
```

</details>
