# riverhog_ftp_adapter.load_config

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-ftp-adapter:riverhog-ftp-adapter-load-config:f8d9f3be9b -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-ftp-adapter](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-9d2dbec4f4"></a>
- <a id="s-36fa9ac05f"></a>`distribution`: `riverhog-ftp-adapter`
- <a id="s-4125da8356"></a>`module`: `riverhog_ftp_adapter`
- <a id="s-4b9d202d2e"></a>`name`: `load_config`
- <a id="s-0628f0d143"></a>`unit`: `export`

### Declared structure

- <a id="s-c700a9f2bd"></a>`kind`: `"function"`
- <a id="s-e292c8c9a3"></a>`signature`: `"\"(path: 'Path \| None' = None) -> 'FtpAdapterConfig'\""`

## Governing policies

- <a id="pa-93482fc560"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-ftp-adapter:riverhog_ftp_adapter](../../../evidence/sources.md#src-8d11f8fa97) — `reference/riverhog/ingress/ftp/src/riverhog_ftp_adapter/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_ftp_adapter.load_config`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 0a240e9347338b6d22e12e70b8f0b7958afbd5d6b75ceb481df7e5cb6a1d0f84 -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(path: 'Path | None' = None) -> 'FtpAdapterConfig'\""
  },
  "distribution": "riverhog-ftp-adapter",
  "module": "riverhog_ftp_adapter",
  "name": "load_config",
  "unit": "export"
}
```
