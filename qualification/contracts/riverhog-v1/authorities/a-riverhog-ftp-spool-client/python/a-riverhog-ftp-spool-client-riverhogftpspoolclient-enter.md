# a_riverhog_ftp_spool_client.RiverhogFtpSpoolClient.__enter__

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:a-riverhog-ftp-spool-client:a-riverhog-ftp-spool-client-riverhogftpsp-030c2ab528:220b853736 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-ftp-spool-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-662470462b"></a>
- <a id="s-d54267d202"></a>`distribution`: `a-riverhog-ftp-spool-client`
- <a id="s-517b9a8df0"></a>`module`: `a_riverhog_ftp_spool_client`
- <a id="s-a57eaa4c36"></a>`name`: `__enter__`
- <a id="s-a453707200"></a>`owner`: `a_riverhog_ftp_spool_client.RiverhogFtpSpoolClient`
- <a id="s-940b1307a3"></a>`unit`: `member`

### Declared structure

- <a id="s-45fb65c7f4"></a>`kind`: `"method"`
- <a id="s-5e49f27946"></a>`signature`: `"\"(self) -> 'Self'\""`

## Maintained corroboration

### Related interface records

- [RiverhogFtpSpoolClient](a-riverhog-ftp-spool-client-riverhogftpspoolclient.md)

## Governing policies

- <a id="pa-38a37af713"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:a-riverhog-ftp-spool-client:a_riverhog_ftp_spool_client](../../../evidence/sources/authorities.md#src-3caa0330b6) — [some-implementations/riverhog/ingress/ftp-api-client/src/a\_riverhog\_ftp\_spool\_client/\_\_init\_\_.py](../../../../../../some-implementations/riverhog/ingress/ftp-api-client/src/a_riverhog_ftp_spool_client/__init__.py)

### Machine authority

- `/external_contract/python/a_riverhog_ftp_spool_client.RiverhogFtpSpoolClient.__enter__`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 5cb73cd99a2216c6bbcd375b00c0fb036ec18144b8a72d24db696aa71b88d3e2 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'Self'\""
  },
  "distribution": "a-riverhog-ftp-spool-client",
  "module": "a_riverhog_ftp_spool_client",
  "name": "__enter__",
  "owner": "a_riverhog_ftp_spool_client.RiverhogFtpSpoolClient",
  "unit": "member"
}
```

</details>
