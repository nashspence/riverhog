# a_riverhog_ftp_spool_client.RiverhogFtpSpoolClient.close

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:a-riverhog-ftp-spool-client:a-riverhog-ftp-spool-client-riverhogftpsp-e3fb06b3a1:89e6c05d70 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-ftp-spool-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-567259827c"></a>
- <a id="s-afbb7e1f29"></a>`distribution`: `a-riverhog-ftp-spool-client`
- <a id="s-17582a0e58"></a>`module`: `a_riverhog_ftp_spool_client`
- <a id="s-fd4651df02"></a>`name`: `close`
- <a id="s-f3fc9171e3"></a>`owner`: `a_riverhog_ftp_spool_client.RiverhogFtpSpoolClient`
- <a id="s-21305eabe7"></a>`unit`: `member`

### Declared structure

- <a id="s-c8ae4cb072"></a>`kind`: `"method"`
- <a id="s-846a1e5203"></a>`signature`: `"\"(self) -> 'None'\""`

## Maintained corroboration

### Related interface records

- [RiverhogFtpSpoolClient](a-riverhog-ftp-spool-client-riverhogftpspoolclient.md)

## Governing policies

- <a id="pa-229531a8d1"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:a-riverhog-ftp-spool-client:a_riverhog_ftp_spool_client](../../../evidence/sources/authorities.md#src-3caa0330b6) — [some-implementations/riverhog/ingress/ftp-api-client/src/a\_riverhog\_ftp\_spool\_client/\_\_init\_\_.py](../../../../../../some-implementations/riverhog/ingress/ftp-api-client/src/a_riverhog_ftp_spool_client/__init__.py)

### Machine authority

- `/external_contract/python/a_riverhog_ftp_spool_client.RiverhogFtpSpoolClient.close`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: be6876b5fb9c9b01e98e91f8f0fe4698398cce74954c393bded309c2cdbeabe5 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'None'\""
  },
  "distribution": "a-riverhog-ftp-spool-client",
  "module": "a_riverhog_ftp_spool_client",
  "name": "close",
  "owner": "a_riverhog_ftp_spool_client.RiverhogFtpSpoolClient",
  "unit": "member"
}
```

</details>
