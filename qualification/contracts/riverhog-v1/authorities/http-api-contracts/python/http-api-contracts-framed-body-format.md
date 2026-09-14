# http_api_contracts.FRAMED_BODY_FORMAT

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:http-api-contracts:http-api-contracts-framed-body-format:7a91508f17 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [http-api-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-4510b46b7a"></a>
- <a id="s-717e896b12"></a>`distribution`: `http-api-contracts`
- <a id="s-60747e4729"></a>`module`: `http_api_contracts`
- <a id="s-3932e8dc29"></a>`name`: `FRAMED_BODY_FORMAT`
- <a id="s-7d772834af"></a>`unit`: `export`

### Declared structure

- <a id="s-82a5aeb0f0"></a>`kind`: `"constant"`
- <a id="s-d7800dbf82"></a>`value`: `"riverhog-json-opaque-framing/v1"`

## Governing policies

- <a id="pa-c03642d41c"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:http-api-contracts:http_api_contracts](../../../evidence/sources.md#src-a522df4cfd) — `packages/http-api-contracts/src/http_api_contracts/__init__.py`

### Machine authority

- `/external_contract/python/http_api_contracts.FRAMED_BODY_FORMAT`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 93a90ae497137ddc96aaa836d3e33969e683c7d78853f71d7bc158f50ef124a9 -->

```json
{
  "contract": {
    "kind": "constant",
    "value": "riverhog-json-opaque-framing/v1"
  },
  "distribution": "http-api-contracts",
  "module": "http_api_contracts",
  "name": "FRAMED_BODY_FORMAT",
  "unit": "export"
}
```
