# http_api_contracts.MAX_BROWSE_TOKEN_BYTES

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:http-api-contracts:http-api-contracts-max-browse-token-bytes:63016da245 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [http-api-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-82707f22f8"></a>
- <a id="s-293d132365"></a>`distribution`: `http-api-contracts`
- <a id="s-25904530f8"></a>`module`: `http_api_contracts`
- <a id="s-6aa21ed364"></a>`name`: `MAX_BROWSE_TOKEN_BYTES`
- <a id="s-e8ff2bc915"></a>`unit`: `export`

### Declared structure

- <a id="s-c3f5044a99"></a>`kind`: `"constant"`
- <a id="s-f66323fcac"></a>`value`: `8192`

## Governing policies

- <a id="pa-c429d4a3fe"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:http-api-contracts:http_api_contracts](../../../evidence/sources.md#src-a522df4cfd) — `packages/http-api-contracts/src/http_api_contracts/__init__.py`

### Machine authority

- `/external_contract/python/http_api_contracts.MAX_BROWSE_TOKEN_BYTES`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 24b7e45f4cdf5a9daa5ba1866dcac571c1aa8495f1b94a2814d915e75ef097c2 -->

```json
{
  "contract": {
    "kind": "constant",
    "value": 8192
  },
  "distribution": "http-api-contracts",
  "module": "http_api_contracts",
  "name": "MAX_BROWSE_TOKEN_BYTES",
  "unit": "export"
}
```

</details>
