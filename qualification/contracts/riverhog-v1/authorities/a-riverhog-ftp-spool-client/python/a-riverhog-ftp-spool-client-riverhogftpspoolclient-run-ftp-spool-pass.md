# a_riverhog_ftp_spool_client.RiverhogFtpSpoolClient.run_ftp_spool_pass

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:a-riverhog-ftp-spool-client:a-riverhog-ftp-spool-client-riverhogftpsp-cca7fa023b:6b951015a5 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-ftp-spool-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-454431f4c0"></a>
- <a id="s-4bdba5435b"></a>`distribution`: `a-riverhog-ftp-spool-client`
- <a id="s-99ddfd7422"></a>`module`: `a_riverhog_ftp_spool_client`
- <a id="s-c7f294fc77"></a>`name`: `run_ftp_spool_pass`
- <a id="s-d90c5c92ff"></a>`owner`: `a_riverhog_ftp_spool_client.RiverhogFtpSpoolClient`
- <a id="s-d7ab2e14d8"></a>`unit`: `member`

### Declared structure

- <a id="s-9078c336d1"></a>`kind`: `"method"`
- <a id="s-6a20eab351"></a>`signature`: `"\"(self) -> 'dict[str, Any]'\""`

## Maintained corroboration

### Related interface records

- [a-riverhog-ftp-spool run](../../a-riverhog-ftp-spool/cli/a-riverhog-ftp-spool-run.md)
- [POST /v1/run](../../a-riverhog-ftp-spool/http-operations/post-v1-run.md)
- [RiverhogFtpSpoolClient](a-riverhog-ftp-spool-client-riverhogftpspoolclient.md)

## Governing policies

- <a id="pa-0dd9551fc4"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:a-riverhog-ftp-spool-client:a_riverhog_ftp_spool_client](../../../evidence/sources/authorities.md#src-3caa0330b6) — [some-implementations/riverhog/ingress/ftp-api-client/src/a\_riverhog\_ftp\_spool\_client/\_\_init\_\_.py](../../../../../../some-implementations/riverhog/ingress/ftp-api-client/src/a_riverhog_ftp_spool_client/__init__.py)
- **Client method:** [some-implementations/riverhog/ingress/ftp-api-client/src/a\_riverhog\_ftp\_spool\_client/client.py::RiverhogFtpSpoolClient.run\_ftp\_spool\_pass](../../../../../../some-implementations/riverhog/ingress/ftp-api-client/src/a_riverhog_ftp_spool_client/client.py#L99)

### Machine authority

- `/external_contract/python/a_riverhog_ftp_spool_client.RiverhogFtpSpoolClient.run_ftp_spool_pass`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 6ac866d2c8429c55a8dd9faf68065d9678b50532457190d6cc2940b4c4343203 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'dict[str, Any]'\""
  },
  "distribution": "a-riverhog-ftp-spool-client",
  "module": "a_riverhog_ftp_spool_client",
  "name": "run_ftp_spool_pass",
  "owner": "a_riverhog_ftp_spool_client.RiverhogFtpSpoolClient",
  "unit": "member"
}
```

</details>
