# riverhog_ftp_adapter_api_client.FtpAdapterApiError

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-ftp-adapter-api-client:riverhog-ftp-adapter-api-client-ftpadapterapierror:0627e07838 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-ftp-adapter-api-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-345c4b585a"></a>
- <a id="s-bb9242f69f"></a>`distribution`: `riverhog-ftp-adapter-api-client`
- <a id="s-a90c20256d"></a>`module`: `riverhog_ftp_adapter_api_client`
- <a id="s-ac5d117821"></a>`name`: `FtpAdapterApiError`
- <a id="s-e3f11d7f45"></a>`unit`: `export`

### Declared structure

- <a id="s-73608848af"></a>`kind`: `"class"`
- <a id="s-f54630b999"></a>`signature`: `"\"(message: 'str', *, code: 'str' = 'ftp_adapter_error', status: 'int \| None' = None) -> 'None'\""`

## Governing policies

- <a id="pa-d27051d23b"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-ftp-adapter-api-client:riverhog_ftp_adapter_api_client](../../../evidence/sources.md#src-a83ae875ae) — [reference/riverhog/ingress/ftp-api-client/src/riverhog\_ftp\_adapter\_api\_client/\_\_init\_\_.py](../../../../../../reference/riverhog/ingress/ftp-api-client/src/riverhog_ftp_adapter_api_client/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_ftp_adapter_api_client.FtpAdapterApiError`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 3e152c885b6fc01cb7681a65895ea235a5656b20d533fc3de31d0cae457b9577 -->

```json
{
  "contract": {
    "kind": "class",
    "signature": "\"(message: 'str', *, code: 'str' = 'ftp_adapter_error', status: 'int | None' = None) -> 'None'\""
  },
  "distribution": "riverhog-ftp-adapter-api-client",
  "module": "riverhog_ftp_adapter_api_client",
  "name": "FtpAdapterApiError",
  "unit": "export"
}
```

</details>
