# a_riverhog_ftp_spool.FtpSpoolConfig.unique_sources

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:a-riverhog-ftp-spool:a-riverhog-ftp-spool-ftpspoolconfig-unique-sources:bb4a3d95dc -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-ftp-spool](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-19da4cfbb6"></a>
- <a id="s-5a4e0de297"></a>`distribution`: `a-riverhog-ftp-spool`
- <a id="s-6bfab36c49"></a>`module`: `a_riverhog_ftp_spool`
- <a id="s-3df217052b"></a>`name`: `unique_sources`
- <a id="s-7225a5bf5b"></a>`owner`: `a_riverhog_ftp_spool.FtpSpoolConfig`
- <a id="s-03acd139bd"></a>`unit`: `member`

### Declared structure

- <a id="s-c011279f58"></a>`kind`: `"classmethod"`
- <a id="s-cb0ad02048"></a>`signature`: `"\"(cls, value: 'tuple[SourceConfig, ...]') -> 'tuple[SourceConfig, ...]'\""`

## Maintained corroboration

### Related interface records

- [FtpSpoolConfig](a-riverhog-ftp-spool-ftpspoolconfig.md)

## Governing policies

- <a id="pa-11bfaf526a"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:a-riverhog-ftp-spool:a_riverhog_ftp_spool](../../../evidence/sources/authorities.md#src-ab8cadf46c) — [some-implementations/riverhog/ingress/ftp/src/a\_riverhog\_ftp\_spool/\_\_init\_\_.py](../../../../../../some-implementations/riverhog/ingress/ftp/src/a_riverhog_ftp_spool/__init__.py)

### Machine authority

- `/external_contract/python/a_riverhog_ftp_spool.FtpSpoolConfig.unique_sources`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 27c8875cde51024aa9201e9400f76ac04c64bf2151aa1c18367058a1a8c90e9b -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, value: 'tuple[SourceConfig, ...]') -> 'tuple[SourceConfig, ...]'\""
  },
  "distribution": "a-riverhog-ftp-spool",
  "module": "a_riverhog_ftp_spool",
  "name": "unique_sources",
  "owner": "a_riverhog_ftp_spool.FtpSpoolConfig",
  "unit": "member"
}
```

</details>
