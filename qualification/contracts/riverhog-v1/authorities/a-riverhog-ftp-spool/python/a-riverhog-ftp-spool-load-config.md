# a_riverhog_ftp_spool.load_config

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:a-riverhog-ftp-spool:a-riverhog-ftp-spool-load-config:9671ca70f5 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-ftp-spool](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-25e02b3212"></a>
- <a id="s-44c354655c"></a>`distribution`: `a-riverhog-ftp-spool`
- <a id="s-65ff1b82ee"></a>`module`: `a_riverhog_ftp_spool`
- <a id="s-f779f5c4a7"></a>`name`: `load_config`
- <a id="s-ff033c8267"></a>`unit`: `export`

### Declared structure

- <a id="s-59de4e3505"></a>`kind`: `"function"`
- <a id="s-c4980d1ed5"></a>`signature`: `"\"(path: 'Path \| None' = None) -> 'FtpSpoolConfig'\""`

## Governing policies

- <a id="pa-31d045e954"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:a-riverhog-ftp-spool:a_riverhog_ftp_spool](../../../evidence/sources/authorities.md#src-ab8cadf46c) — [some-implementations/riverhog/ingress/ftp/src/a\_riverhog\_ftp\_spool/\_\_init\_\_.py](../../../../../../some-implementations/riverhog/ingress/ftp/src/a_riverhog_ftp_spool/__init__.py)

### Machine authority

- `/external_contract/python/a_riverhog_ftp_spool.load_config`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: d0add000e9f697174165d11100f77d1f66f449291dc77fcdceef047fbcee6d0b -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(path: 'Path | None' = None) -> 'FtpSpoolConfig'\""
  },
  "distribution": "a-riverhog-ftp-spool",
  "module": "a_riverhog_ftp_spool",
  "name": "load_config",
  "unit": "export"
}
```

</details>
