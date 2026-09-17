# riverhog_client.configured_upload_concurrency

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-configured-upload-concurrency:c0e7b1c803 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-d7c4e25535"></a>
- <a id="s-a1943d0a75"></a>`distribution`: `riverhog-client`
- <a id="s-4cb09f3b6d"></a>`module`: `riverhog_client`
- <a id="s-b8efb8b34b"></a>`name`: `configured_upload_concurrency`
- <a id="s-dfa46f34c8"></a>`unit`: `export`

### Declared structure

- <a id="s-e33be1ff15"></a>`kind`: `"function"`
- <a id="s-ea21aecd26"></a>`signature`: `"\"(values: 'Mapping[str, str] \| None' = None) -> 'int'\""`

## Governing policies

- <a id="pa-cd86ebeb6c"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-client:riverhog_client](../../../evidence/sources/authorities.md#src-c149020c71) — [packages/riverhog-client/src/riverhog\_client/\_\_init\_\_.py](../../../../../../packages/riverhog-client/src/riverhog_client/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_client.configured_upload_concurrency`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: f93d20bf2b20cb610b63fcd947d4ce303668dee21adea0ab4ebb4efb68dcf1ac -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(values: 'Mapping[str, str] | None' = None) -> 'int'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client",
  "name": "configured_upload_concurrency",
  "unit": "export"
}
```

</details>
