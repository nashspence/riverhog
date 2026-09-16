# riverhog_client.configured_download_concurrency

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-configured-download-concurrency:45b0ed90c0 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-a1b87a643c"></a>
- <a id="s-acc72de967"></a>`distribution`: `riverhog-client`
- <a id="s-4bf1f4a4f2"></a>`module`: `riverhog_client`
- <a id="s-e4985f789d"></a>`name`: `configured_download_concurrency`
- <a id="s-c0582aec39"></a>`unit`: `export`

### Declared structure

- <a id="s-86928d70ed"></a>`kind`: `"function"`
- <a id="s-e40e5479c6"></a>`signature`: `"\"(values: 'Mapping[str, str] \| None' = None) -> 'int'\""`

## Governing policies

- <a id="pa-6611009429"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-client:riverhog_client](../../../evidence/sources.md#src-c149020c71) — `packages/riverhog-client/src/riverhog_client/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_client.configured_download_concurrency`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 3985b7fe7985dfe349fc3192e1b5e2a96205cacb9f599469e14c259d1ecb803a -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(values: 'Mapping[str, str] | None' = None) -> 'int'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client",
  "name": "configured_download_concurrency",
  "unit": "export"
}
```

</details>
