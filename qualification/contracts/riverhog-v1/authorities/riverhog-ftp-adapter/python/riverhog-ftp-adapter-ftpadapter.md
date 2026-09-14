# riverhog_ftp_adapter.FtpAdapter

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-ftp-adapter:riverhog-ftp-adapter-ftpadapter:868bb0c730 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-ftp-adapter](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-aa6bdb8258"></a>
- <a id="s-68d22b36e2"></a>`distribution`: `riverhog-ftp-adapter`
- <a id="s-ecf03413ea"></a>`module`: `riverhog_ftp_adapter`
- <a id="s-141a98cfa7"></a>`name`: `FtpAdapter`
- <a id="s-0ccf1cb2e5"></a>`unit`: `export`

### Declared structure

- <a id="s-7be848eff6"></a>`kind`: `"class"`
- <a id="s-d5d613712c"></a>`signature`: `"\"(api: 'ApiClient', config: 'FtpAdapterConfig', *, provenance_observer_factory: 'FileStateObserverFactory \| None' = None) -> 'None'\""`

## Maintained corroboration

### Related interface records

- [accept_completed_file](riverhog-ftp-adapter-ftpadapter-accept-completed-file.md)
- [flush](riverhog-ftp-adapter-ftpadapter-flush.md)
- [run_once](riverhog-ftp-adapter-ftpadapter-run-once.md)
- [status](riverhog-ftp-adapter-ftpadapter-status.md)

## Governing policies

- <a id="pa-2dd86172fb"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-ftp-adapter:riverhog_ftp_adapter](../../../evidence/sources.md#src-8d11f8fa97) — `reference/riverhog/ingress/ftp/src/riverhog_ftp_adapter/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_ftp_adapter.FtpAdapter`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: d568a977e6ed9e2f25cfee5ea5d83b9270266da1a2616f85bddc7324388f7cb3 -->

```json
{
  "contract": {
    "kind": "class",
    "signature": "\"(api: 'ApiClient', config: 'FtpAdapterConfig', *, provenance_observer_factory: 'FileStateObserverFactory | None' = None) -> 'None'\""
  },
  "distribution": "riverhog-ftp-adapter",
  "module": "riverhog_ftp_adapter",
  "name": "FtpAdapter",
  "unit": "export"
}
```
