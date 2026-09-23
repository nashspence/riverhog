# a_riverhog_ftp_spool.FtpSpool.flush

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:a-riverhog-ftp-spool:a-riverhog-ftp-spool-ftpspool-flush:df920d2c28 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-ftp-spool](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-602aed5386"></a>
- <a id="s-9779a747f0"></a>`distribution`: `a-riverhog-ftp-spool`
- <a id="s-6737d88b61"></a>`module`: `a_riverhog_ftp_spool`
- <a id="s-6f040448a0"></a>`name`: `flush`
- <a id="s-4a8abfbe84"></a>`owner`: `a_riverhog_ftp_spool.FtpSpool`
- <a id="s-6f9a18da60"></a>`unit`: `member`

### Declared structure

- <a id="s-2a19a814fb"></a>`kind`: `"method"`
- <a id="s-7007580f66"></a>`signature`: `"\"(self, source_id: 'str') -> 'dict[str, object]'\""`

## Maintained corroboration

### Related interface records

- [FtpSpool](a-riverhog-ftp-spool-ftpspool.md)

## Governing policies

- <a id="pa-01777100a8"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:a-riverhog-ftp-spool:a_riverhog_ftp_spool](../../../evidence/sources/authorities.md#src-ab8cadf46c) — [some-implementations/riverhog/ingress/ftp/src/a\_riverhog\_ftp\_spool/\_\_init\_\_.py](../../../../../../some-implementations/riverhog/ingress/ftp/src/a_riverhog_ftp_spool/__init__.py)

### Machine authority

- `/external_contract/python/a_riverhog_ftp_spool.FtpSpool.flush`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 8edb1193ab3a37f294f877ab1cebd9f5f55020ae6f8ba9967b0a4c40a9a65a7f -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, source_id: 'str') -> 'dict[str, object]'\""
  },
  "distribution": "a-riverhog-ftp-spool",
  "module": "a_riverhog_ftp_spool",
  "name": "flush",
  "owner": "a_riverhog_ftp_spool.FtpSpool",
  "unit": "member"
}
```

</details>
