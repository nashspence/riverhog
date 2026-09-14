# stove0_review_sampler_client.SamplerProtocolError

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-review-sampler-client:stove0-review-sampler-client-samplerprotocolerror:d2bf79dee2 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-review-sampler-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-1a70bf78aa"></a>
- <a id="s-43ecc6f5c0"></a>`distribution`: `stove0-review-sampler-client`
- <a id="s-8f8e73654d"></a>`module`: `stove0_review_sampler_client`
- <a id="s-f112cff23d"></a>`name`: `SamplerProtocolError`
- <a id="s-e3145a0bce"></a>`unit`: `export`

### Declared structure

- <a id="s-af43dd7005"></a>`kind`: `"class"`
- <a id="s-24ac5e3a78"></a>`signature`: `"'(message: \\'str\\', *, failure_kind: \"Literal[\\'remote_rejection\\', \\'transport\\', \\'invalid_response\\']\", code: \\'str \| None\\' = None, observed_status: \\'int \| None\\' = None, details: \\'Mapping[str, Any] \| None\\' = None) -> \\'None\\''"`

## Governing policies

- <a id="pa-85858b4546"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-review-sampler-client:stove0_review_sampler_client](../../../evidence/sources.md#src-4a777c675f) — `reference/stove0/targets/review/sampler/client/src/stove0_review_sampler_client/__init__.py`

### Machine authority

- `/external_contract/python/stove0_review_sampler_client.SamplerProtocolError`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 533f334927bbb8aafaf92c4fa1e74be9cd1eef2df8beacd2c023e2697c5f5d5a -->

```json
{
  "contract": {
    "kind": "class",
    "signature": "'(message: \\'str\\', *, failure_kind: \"Literal[\\'remote_rejection\\', \\'transport\\', \\'invalid_response\\']\", code: \\'str | None\\' = None, observed_status: \\'int | None\\' = None, details: \\'Mapping[str, Any] | None\\' = None) -> \\'None\\''"
  },
  "distribution": "stove0-review-sampler-client",
  "module": "stove0_review_sampler_client",
  "name": "SamplerProtocolError",
  "unit": "export"
}
```
