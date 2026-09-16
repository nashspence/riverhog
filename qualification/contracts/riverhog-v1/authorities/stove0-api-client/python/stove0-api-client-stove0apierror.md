# stove0_api_client.Stove0ApiError

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-api-client:stove0-api-client-stove0apierror:b7dc9fd78f -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-api-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-32758165dd"></a>
- <a id="s-91f12d39c0"></a>`distribution`: `stove0-api-client`
- <a id="s-6ec970c302"></a>`module`: `stove0_api_client`
- <a id="s-9578467e34"></a>`name`: `Stove0ApiError`
- <a id="s-1cfc8e7dab"></a>`unit`: `export`

### Declared structure

- <a id="s-b6e990aade"></a>`kind`: `"class"`
- <a id="s-0423fa905b"></a>`signature`: `"\"(message: 'str', *, code: 'str' = 'stove0_client_error', observed_status: 'int \| None' = None, details: 'Mapping[str, Any] \| None' = None) -> 'None'\""`

## Governing policies

- <a id="pa-0664060147"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-api-client:stove0_api_client](../../../evidence/sources.md#src-5d52ac5998) — `reference/stove0/packages/api-client/src/stove0_api_client/__init__.py`

### Machine authority

- `/external_contract/python/stove0_api_client.Stove0ApiError`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 2644508ef9d420b023acb0a1e0560551890b74a50905764b46d5d30029df4c35 -->

```json
{
  "contract": {
    "kind": "class",
    "signature": "\"(message: 'str', *, code: 'str' = 'stove0_client_error', observed_status: 'int | None' = None, details: 'Mapping[str, Any] | None' = None) -> 'None'\""
  },
  "distribution": "stove0-api-client",
  "module": "stove0_api_client",
  "name": "Stove0ApiError",
  "unit": "export"
}
```

</details>
