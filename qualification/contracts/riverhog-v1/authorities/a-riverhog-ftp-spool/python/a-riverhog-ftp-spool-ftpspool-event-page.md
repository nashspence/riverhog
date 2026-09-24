# a_riverhog_ftp_spool.FtpSpool.event_page

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:a-riverhog-ftp-spool:a-riverhog-ftp-spool-ftpspool-event-page:a0db251a44 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-ftp-spool](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-95a9a09c8c"></a>
- <a id="s-cc313bd897"></a>`distribution`: `a-riverhog-ftp-spool`
- <a id="s-f89151515e"></a>`module`: `a_riverhog_ftp_spool`
- <a id="s-123511c46b"></a>`name`: `event_page`
- <a id="s-5b58b00a0f"></a>`owner`: `a_riverhog_ftp_spool.FtpSpool`
- <a id="s-357ecd8b7a"></a>`unit`: `member`

### Declared structure

- <a id="s-b17253ffbc"></a>`kind`: `"method"`
- <a id="s-a29719bf48"></a>`signature`: `"\"(self, source_id: 'str', *, after: 'str \| None', limit: 'int') -> 'FtpEventPage'\""`

## Maintained corroboration

### Related interface records

- [FtpSpool](a-riverhog-ftp-spool-ftpspool.md)

## Governing policies

- <a id="pa-fbfc2ca4e6"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:a-riverhog-ftp-spool:a_riverhog_ftp_spool](../../../evidence/sources/authorities.md#src-ab8cadf46c) — [some-implementations/riverhog/ingress/ftp/src/a\_riverhog\_ftp\_spool/\_\_init\_\_.py](../../../../../../some-implementations/riverhog/ingress/ftp/src/a_riverhog_ftp_spool/__init__.py)

### Machine authority

- `/external_contract/python/a_riverhog_ftp_spool.FtpSpool.event_page`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 5a28c745865acf14bcbdca4a9ed1e7e85e9e04d17f25631a7886d11c976ff4e5 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, source_id: 'str', *, after: 'str | None', limit: 'int') -> 'FtpEventPage'\""
  },
  "distribution": "a-riverhog-ftp-spool",
  "module": "a_riverhog_ftp_spool",
  "name": "event_page",
  "owner": "a_riverhog_ftp_spool.FtpSpool",
  "unit": "member"
}
```

</details>
