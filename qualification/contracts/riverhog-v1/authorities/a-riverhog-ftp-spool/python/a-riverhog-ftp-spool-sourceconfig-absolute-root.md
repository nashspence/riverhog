# a_riverhog_ftp_spool.SourceConfig.absolute_root

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:a-riverhog-ftp-spool:a-riverhog-ftp-spool-sourceconfig-absolute-root:f7013ad54d -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-ftp-spool](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-f987abce88"></a>
- <a id="s-ae54e5e093"></a>`distribution`: `a-riverhog-ftp-spool`
- <a id="s-516c8af5a5"></a>`module`: `a_riverhog_ftp_spool`
- <a id="s-67a7572616"></a>`name`: `absolute_root`
- <a id="s-cd8535376c"></a>`owner`: `a_riverhog_ftp_spool.SourceConfig`
- <a id="s-62d6bcc10e"></a>`unit`: `member`

### Declared structure

- <a id="s-4dd7d56edf"></a>`kind`: `"classmethod"`
- <a id="s-c41f5d56a7"></a>`signature`: `"\"(cls, value: 'Path') -> 'Path'\""`

## Maintained corroboration

### Related interface records

- [SourceConfig](a-riverhog-ftp-spool-sourceconfig.md)

## Governing policies

- <a id="pa-c4bd3ee40e"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:a-riverhog-ftp-spool:a_riverhog_ftp_spool](../../../evidence/sources/authorities.md#src-ab8cadf46c) — [some-implementations/riverhog/ingress/ftp/src/a\_riverhog\_ftp\_spool/\_\_init\_\_.py](../../../../../../some-implementations/riverhog/ingress/ftp/src/a_riverhog_ftp_spool/__init__.py)

### Machine authority

- `/external_contract/python/a_riverhog_ftp_spool.SourceConfig.absolute_root`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: ef8dfb9247202b2ecdd087bead9621ff036134e6f0b8d70fb95369eed0b0bda9 -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, value: 'Path') -> 'Path'\""
  },
  "distribution": "a-riverhog-ftp-spool",
  "module": "a_riverhog_ftp_spool",
  "name": "absolute_root",
  "owner": "a_riverhog_ftp_spool.SourceConfig",
  "unit": "member"
}
```

</details>
