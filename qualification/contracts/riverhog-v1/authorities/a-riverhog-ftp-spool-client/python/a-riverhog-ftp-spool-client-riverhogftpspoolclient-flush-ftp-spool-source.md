# a_riverhog_ftp_spool_client.RiverhogFtpSpoolClient.flush_ftp_spool_source

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:a-riverhog-ftp-spool-client:a-riverhog-ftp-spool-client-riverhogftpsp-4595c5a63e:cbeb0bec99 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-ftp-spool-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-b1a7e5a53d"></a>
- <a id="s-840ac84a3b"></a>`distribution`: `a-riverhog-ftp-spool-client`
- <a id="s-54327b1be4"></a>`module`: `a_riverhog_ftp_spool_client`
- <a id="s-f830e2326f"></a>`name`: `flush_ftp_spool_source`
- <a id="s-4fe3b17805"></a>`owner`: `a_riverhog_ftp_spool_client.RiverhogFtpSpoolClient`
- <a id="s-36d7dfdc85"></a>`unit`: `member`

### Declared structure

- <a id="s-4336f42612"></a>`kind`: `"method"`
- <a id="s-6899d301a4"></a>`signature`: `"\"(self, source_id: 'str') -> 'dict[str, Any]'\""`

## Maintained corroboration

### Related interface records

- [a-riverhog-ftp-spool flush](../../a-riverhog-ftp-spool/cli/a-riverhog-ftp-spool-flush.md)
- [POST /v1/sources/{source_id}/flush](../../a-riverhog-ftp-spool/http-operations/post-v1-sources-source-id-flush.md)
- [RiverhogFtpSpoolClient](a-riverhog-ftp-spool-client-riverhogftpspoolclient.md)

## Governing policies

- <a id="pa-ffb82b367c"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:a-riverhog-ftp-spool-client:a_riverhog_ftp_spool_client](../../../evidence/sources/authorities.md#src-3caa0330b6) — [some-implementations/riverhog/ingress/ftp-api-client/src/a\_riverhog\_ftp\_spool\_client/\_\_init\_\_.py](../../../../../../some-implementations/riverhog/ingress/ftp-api-client/src/a_riverhog_ftp_spool_client/__init__.py)
- **Client method:** [some-implementations/riverhog/ingress/ftp-api-client/src/a\_riverhog\_ftp\_spool\_client/client.py::RiverhogFtpSpoolClient.flush\_ftp\_spool\_source](../../../../../../some-implementations/riverhog/ingress/ftp-api-client/src/a_riverhog_ftp_spool_client/client.py#L118)

### Machine authority

- `/external_contract/python/a_riverhog_ftp_spool_client.RiverhogFtpSpoolClient.flush_ftp_spool_source`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 6298a2eccb3b562e3e6d1ce754d06f3fbda49e91257a826ef19eaa075dd30211 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, source_id: 'str') -> 'dict[str, Any]'\""
  },
  "distribution": "a-riverhog-ftp-spool-client",
  "module": "a_riverhog_ftp_spool_client",
  "name": "flush_ftp_spool_source",
  "owner": "a_riverhog_ftp_spool_client.RiverhogFtpSpoolClient",
  "unit": "member"
}
```

</details>
