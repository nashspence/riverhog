# http_api_contracts.FRAMED_BODY_MEDIA_TYPE

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:http-api-contracts:http-api-contracts-framed-body-media-type:7b467752ab -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [http-api-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-f67b705ea1"></a>
- <a id="s-b218287a3f"></a>`distribution`: `http-api-contracts`
- <a id="s-2ba394caf0"></a>`module`: `http_api_contracts`
- <a id="s-4e9ff3fe60"></a>`name`: `FRAMED_BODY_MEDIA_TYPE`
- <a id="s-d477653232"></a>`unit`: `export`

### Declared structure

- <a id="s-01ad930323"></a>`kind`: `"constant"`
- <a id="s-eada3a5f3a"></a>`value`: `"application/vnd.riverhog.json-opaque-framing"`

## Governing policies

- <a id="pa-41441d6f82"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:http-api-contracts:http_api_contracts](../../../evidence/sources.md#src-a522df4cfd) — `packages/http-api-contracts/src/http_api_contracts/__init__.py`

### Machine authority

- `/external_contract/python/http_api_contracts.FRAMED_BODY_MEDIA_TYPE`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 490375a225fd8fb9bfbe09f24c440489f551ec608e21b6192a465f8d330fb65b -->

```json
{
  "contract": {
    "kind": "constant",
    "value": "application/vnd.riverhog.json-opaque-framing"
  },
  "distribution": "http-api-contracts",
  "module": "http_api_contracts",
  "name": "FRAMED_BODY_MEDIA_TYPE",
  "unit": "export"
}
```
