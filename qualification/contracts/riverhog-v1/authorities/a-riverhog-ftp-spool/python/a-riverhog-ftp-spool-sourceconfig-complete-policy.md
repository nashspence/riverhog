# a_riverhog_ftp_spool.SourceConfig.complete_policy

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:a-riverhog-ftp-spool:a-riverhog-ftp-spool-sourceconfig-complete-policy:733f46d206 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-ftp-spool](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-537a21f37c"></a>
- <a id="s-52517f05d4"></a>`distribution`: `a-riverhog-ftp-spool`
- <a id="s-36113f669d"></a>`module`: `a_riverhog_ftp_spool`
- <a id="s-934c25a34a"></a>`name`: `complete_policy`
- <a id="s-791b32e41c"></a>`owner`: `a_riverhog_ftp_spool.SourceConfig`
- <a id="s-178c5eb670"></a>`unit`: `member`

### Declared structure

- <a id="s-33f153b5cf"></a>`kind`: `"method"`
- <a id="s-3c5488a6c2"></a>`signature`: `"\"(self) -> 'Self'\""`

## Maintained corroboration

### Related interface records

- [SourceConfig](a-riverhog-ftp-spool-sourceconfig.md)

## Governing policies

- <a id="pa-7f7cd32b10"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:a-riverhog-ftp-spool:a_riverhog_ftp_spool](../../../evidence/sources/authorities.md#src-ab8cadf46c) — [some-implementations/riverhog/ingress/ftp/src/a\_riverhog\_ftp\_spool/\_\_init\_\_.py](../../../../../../some-implementations/riverhog/ingress/ftp/src/a_riverhog_ftp_spool/__init__.py)

### Machine authority

- `/external_contract/python/a_riverhog_ftp_spool.SourceConfig.complete_policy`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: cce56aab31bc61d5872c5bdc53f773414564a362e6cc0351200044baa528fe64 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'Self'\""
  },
  "distribution": "a-riverhog-ftp-spool",
  "module": "a_riverhog_ftp_spool",
  "name": "complete_policy",
  "owner": "a_riverhog_ftp_spool.SourceConfig",
  "unit": "member"
}
```

</details>
