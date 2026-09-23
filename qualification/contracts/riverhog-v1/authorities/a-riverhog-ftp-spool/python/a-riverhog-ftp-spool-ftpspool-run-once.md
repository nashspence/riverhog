# a_riverhog_ftp_spool.FtpSpool.run_once

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:a-riverhog-ftp-spool:a-riverhog-ftp-spool-ftpspool-run-once:79a535d3b9 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-ftp-spool](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-cc02254836"></a>
- <a id="s-a78cdf9f46"></a>`distribution`: `a-riverhog-ftp-spool`
- <a id="s-d38589154b"></a>`module`: `a_riverhog_ftp_spool`
- <a id="s-2194a5102e"></a>`name`: `run_once`
- <a id="s-5bafc19772"></a>`owner`: `a_riverhog_ftp_spool.FtpSpool`
- <a id="s-92a10fdc8e"></a>`unit`: `member`

### Declared structure

- <a id="s-463d983168"></a>`kind`: `"method"`
- <a id="s-2601d14139"></a>`signature`: `"\"(self, source_ids: 'Sequence[str] \| None' = None) -> 'dict[str, object]'\""`

## Maintained corroboration

### Related interface records

- [FtpSpool](a-riverhog-ftp-spool-ftpspool.md)

## Governing policies

- <a id="pa-c3f060e13b"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:a-riverhog-ftp-spool:a_riverhog_ftp_spool](../../../evidence/sources/authorities.md#src-ab8cadf46c) — [some-implementations/riverhog/ingress/ftp/src/a\_riverhog\_ftp\_spool/\_\_init\_\_.py](../../../../../../some-implementations/riverhog/ingress/ftp/src/a_riverhog_ftp_spool/__init__.py)

### Machine authority

- `/external_contract/python/a_riverhog_ftp_spool.FtpSpool.run_once`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 881d882fb03cab7f14c9d05d679490e49841a06f8117d806efe8ffe18bfefec6 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, source_ids: 'Sequence[str] | None' = None) -> 'dict[str, object]'\""
  },
  "distribution": "a-riverhog-ftp-spool",
  "module": "a_riverhog_ftp_spool",
  "name": "run_once",
  "owner": "a_riverhog_ftp_spool.FtpSpool",
  "unit": "member"
}
```

</details>
