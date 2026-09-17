# riverhog_client.configured_download_window

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-configured-download-window:cda75e5bd6 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-4b8c8b1dd4"></a>
- <a id="s-11b564746f"></a>`distribution`: `riverhog-client`
- <a id="s-b06678fa14"></a>`module`: `riverhog_client`
- <a id="s-8bd4d43f91"></a>`name`: `configured_download_window`
- <a id="s-8ebae55c17"></a>`unit`: `export`

### Declared structure

- <a id="s-1f9f5a4bb3"></a>`kind`: `"function"`
- <a id="s-a7622a2b33"></a>`signature`: `"\"(values: 'Mapping[str, str] \| None' = None, *, concurrency: 'int \| None' = None) -> 'int'\""`

## Governing policies

- <a id="pa-814f8d60a3"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-client:riverhog_client](../../../evidence/sources/authorities.md#src-c149020c71) — [packages/riverhog-client/src/riverhog\_client/\_\_init\_\_.py](../../../../../../packages/riverhog-client/src/riverhog_client/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_client.configured_download_window`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 91fe91c65cad4d37bc617d00f362ab1823333239a6609d8ef0e8ffd80b4301f8 -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(values: 'Mapping[str, str] | None' = None, *, concurrency: 'int | None' = None) -> 'int'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client",
  "name": "configured_download_window",
  "unit": "export"
}
```

</details>
