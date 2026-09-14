# http_api_contracts.iter_json_sequence_records

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:http-api-contracts:http-api-contracts-iter-json-sequence-records:4c20fc7314 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [http-api-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-dc5ca11dd0"></a>
- <a id="s-9fa6f5e933"></a>`distribution`: `http-api-contracts`
- <a id="s-c5b45a0861"></a>`module`: `http_api_contracts`
- <a id="s-b126678b09"></a>`name`: `iter_json_sequence_records`
- <a id="s-6923a87f60"></a>`unit`: `export`

### Declared structure

- <a id="s-295b3b72e4"></a>`kind`: `"function"`
- <a id="s-02b012f114"></a>`signature`: `"\"(chunks: 'Iterable[bytes]') -> 'Iterator[dict[str, Any]]'\""`

## Governing policies

- <a id="pa-e517c3296a"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:http-api-contracts:http_api_contracts](../../../evidence/sources.md#src-a522df4cfd) — `packages/http-api-contracts/src/http_api_contracts/__init__.py`

### Machine authority

- `/external_contract/python/http_api_contracts.iter_json_sequence_records`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: a495cfacbea22ef305aad763df37fd91f0603d3802ddd323bbfaf098b7794d29 -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(chunks: 'Iterable[bytes]') -> 'Iterator[dict[str, Any]]'\""
  },
  "distribution": "http-api-contracts",
  "module": "http_api_contracts",
  "name": "iter_json_sequence_records",
  "unit": "export"
}
```
