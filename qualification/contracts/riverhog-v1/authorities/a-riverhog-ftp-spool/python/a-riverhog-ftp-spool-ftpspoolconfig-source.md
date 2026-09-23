# a_riverhog_ftp_spool.FtpSpoolConfig.source

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:a-riverhog-ftp-spool:a-riverhog-ftp-spool-ftpspoolconfig-source:5a6579a8ea -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-ftp-spool](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-3e35aedb07"></a>
- <a id="s-446e1dd6d9"></a>`distribution`: `a-riverhog-ftp-spool`
- <a id="s-49c4c47bdb"></a>`module`: `a_riverhog_ftp_spool`
- <a id="s-3e372d4992"></a>`name`: `source`
- <a id="s-9152e5fa15"></a>`owner`: `a_riverhog_ftp_spool.FtpSpoolConfig`
- <a id="s-ec24776083"></a>`unit`: `member`

### Declared structure

- <a id="s-83805806de"></a>`kind`: `"method"`
- <a id="s-2e9d6e94ec"></a>`signature`: `"\"(self, source_id: 'str') -> 'SourceConfig'\""`

## Maintained corroboration

### Related interface records

- [FtpSpoolConfig](a-riverhog-ftp-spool-ftpspoolconfig.md)

## Governing policies

- <a id="pa-1c62e79557"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:a-riverhog-ftp-spool:a_riverhog_ftp_spool](../../../evidence/sources/authorities.md#src-ab8cadf46c) — [some-implementations/riverhog/ingress/ftp/src/a\_riverhog\_ftp\_spool/\_\_init\_\_.py](../../../../../../some-implementations/riverhog/ingress/ftp/src/a_riverhog_ftp_spool/__init__.py)

### Machine authority

- `/external_contract/python/a_riverhog_ftp_spool.FtpSpoolConfig.source`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 000c9e817bce46b0462ebcbe69ed9b9f0d2e8201d5b41046dedcfc846f09c6a1 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, source_id: 'str') -> 'SourceConfig'\""
  },
  "distribution": "a-riverhog-ftp-spool",
  "module": "a_riverhog_ftp_spool",
  "name": "source",
  "owner": "a_riverhog_ftp_spool.FtpSpoolConfig",
  "unit": "member"
}
```

</details>
