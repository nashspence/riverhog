# a_riverhog_ftp_spool.FtpSpool

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:a-riverhog-ftp-spool:a-riverhog-ftp-spool-ftpspool:4fc96b159b -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-ftp-spool](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-30e2d50dcf"></a>
- <a id="s-645ab6dd93"></a>`distribution`: `a-riverhog-ftp-spool`
- <a id="s-3b03600435"></a>`module`: `a_riverhog_ftp_spool`
- <a id="s-252c6c9ea9"></a>`name`: `FtpSpool`
- <a id="s-7f6fa36762"></a>`unit`: `export`

### Declared structure

- <a id="s-7294d8447d"></a>`kind`: `"class"`
- <a id="s-90f7fd7cf2"></a>`signature`: `"\"(api: 'ApiClient', config: 'FtpSpoolConfig', *, provenance_observer_factory: 'FileStateObserverFactory \| None' = None) -> 'None'\""`

## Maintained corroboration

### Related interface records

- [accept_completed_file](a-riverhog-ftp-spool-ftpspool-accept-completed-file.md)
- [flush](a-riverhog-ftp-spool-ftpspool-flush.md)
- [run_once](a-riverhog-ftp-spool-ftpspool-run-once.md)
- [status](a-riverhog-ftp-spool-ftpspool-status.md)

## Governing policies

- <a id="pa-2c2728dffc"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:a-riverhog-ftp-spool:a_riverhog_ftp_spool](../../../evidence/sources/authorities.md#src-ab8cadf46c) — [some-implementations/riverhog/ingress/ftp/src/a\_riverhog\_ftp\_spool/\_\_init\_\_.py](../../../../../../some-implementations/riverhog/ingress/ftp/src/a_riverhog_ftp_spool/__init__.py)

### Machine authority

- `/external_contract/python/a_riverhog_ftp_spool.FtpSpool`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: bdfc200244e54b7e5d00067efd9121a5bf0c7242a44899de1e556dd26636fdd7 -->

```json
{
  "contract": {
    "kind": "class",
    "signature": "\"(api: 'ApiClient', config: 'FtpSpoolConfig', *, provenance_observer_factory: 'FileStateObserverFactory | None' = None) -> 'None'\""
  },
  "distribution": "a-riverhog-ftp-spool",
  "module": "a_riverhog_ftp_spool",
  "name": "FtpSpool",
  "unit": "export"
}
```

</details>
