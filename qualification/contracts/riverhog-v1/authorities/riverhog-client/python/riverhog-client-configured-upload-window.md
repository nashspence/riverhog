# riverhog_client.configured_upload_window

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-configured-upload-window:9a4a607756 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-8bb0abdd99"></a>
- <a id="s-1eac9e4761"></a>`distribution`: `riverhog-client`
- <a id="s-133f0a7e3f"></a>`module`: `riverhog_client`
- <a id="s-2e05f4e67c"></a>`name`: `configured_upload_window`
- <a id="s-91af556b77"></a>`unit`: `export`

### Declared structure

- <a id="s-845a4b4ba6"></a>`kind`: `"function"`
- <a id="s-01ab8afd31"></a>`signature`: `"\"(values: 'Mapping[str, str] \| None' = None, *, concurrency: 'int \| None' = None) -> 'int'\""`

## Governing policies

- <a id="pa-07cfe31ed2"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-client:riverhog_client](../../../evidence/sources/authorities.md#src-c149020c71) — [packages/riverhog-client/src/riverhog\_client/\_\_init\_\_.py](../../../../../../packages/riverhog-client/src/riverhog_client/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_client.configured_upload_window`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: dfa37fd6d419b59bb31caaa3ebb2acab90e4cfb64b6fe19237750eb12e1676fa -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(values: 'Mapping[str, str] | None' = None, *, concurrency: 'int | None' = None) -> 'int'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client",
  "name": "configured_upload_window",
  "unit": "export"
}
```

</details>
