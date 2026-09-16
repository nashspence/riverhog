# riverhog_client.Unauthorized

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-unauthorized:bdb77a316c -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-d43d5b445f"></a>
- <a id="s-6bf61b61a1"></a>`distribution`: `riverhog-client`
- <a id="s-a52af19b6e"></a>`module`: `riverhog_client`
- <a id="s-e758921250"></a>`name`: `Unauthorized`
- <a id="s-e88a3bb459"></a>`unit`: `export`

### Declared structure

- <a id="s-6126a54c2c"></a>`kind`: `"class"`
- <a id="s-e474c570c8"></a>`signature`: `"\"(message: 'str', *, code: 'str \| None' = None, observed_status: 'int \| None' = None, details: 'dict[str, Any] \| None' = None) -> 'None'\""`

## Governing policies

- <a id="pa-a3b20c9f6c"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-client:riverhog_client](../../../evidence/sources.md#src-c149020c71) — `packages/riverhog-client/src/riverhog_client/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_client.Unauthorized`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 6a3f57828f7ca6c272a6f2e8a44e665d0431242c028647c8996a4c4c99c3ac54 -->

```json
{
  "contract": {
    "kind": "class",
    "signature": "\"(message: 'str', *, code: 'str | None' = None, observed_status: 'int | None' = None, details: 'dict[str, Any] | None' = None) -> 'None'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client",
  "name": "Unauthorized",
  "unit": "export"
}
```

</details>
