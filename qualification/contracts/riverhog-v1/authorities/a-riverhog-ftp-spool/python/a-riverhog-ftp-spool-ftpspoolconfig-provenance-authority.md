# a_riverhog_ftp_spool.FtpSpoolConfig.provenance_authority

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:a-riverhog-ftp-spool:a-riverhog-ftp-spool-ftpspoolconfig-prove-2d74ac17be:a62c9121d9 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-ftp-spool](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-dc11e1a0e0"></a>
- <a id="s-74f30cb540"></a>`distribution`: `a-riverhog-ftp-spool`
- <a id="s-8452c3a848"></a>`module`: `a_riverhog_ftp_spool`
- <a id="s-b2963c3dc6"></a>`name`: `provenance_authority`
- <a id="s-5df37cfd8e"></a>`owner`: `a_riverhog_ftp_spool.FtpSpoolConfig`
- <a id="s-879b61aa4b"></a>`unit`: `member`

### Declared structure

- <a id="s-a1441f95cc"></a>`kind`: `"method"`
- <a id="s-163c74707a"></a>`signature`: `"\"(self) -> 'Self'\""`

## Maintained corroboration

### Related interface records

- [FtpSpoolConfig](a-riverhog-ftp-spool-ftpspoolconfig.md)

## Governing policies

- <a id="pa-b0c55eab12"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:a-riverhog-ftp-spool:a_riverhog_ftp_spool](../../../evidence/sources/authorities.md#src-ab8cadf46c) — [some-implementations/riverhog/ingress/ftp/src/a\_riverhog\_ftp\_spool/\_\_init\_\_.py](../../../../../../some-implementations/riverhog/ingress/ftp/src/a_riverhog_ftp_spool/__init__.py)

### Machine authority

- `/external_contract/python/a_riverhog_ftp_spool.FtpSpoolConfig.provenance_authority`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 49cdbd8de81d189e8525c49ae5a760cd713ea707a464c7dde281910e0a68b3bb -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'Self'\""
  },
  "distribution": "a-riverhog-ftp-spool",
  "module": "a_riverhog_ftp_spool",
  "name": "provenance_authority",
  "owner": "a_riverhog_ftp_spool.FtpSpoolConfig",
  "unit": "member"
}
```

</details>
