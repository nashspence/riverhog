# a_riverhog_ftp_spool_client.RiverhogFtpSpoolClient.ftp_spool_health_ready

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:a-riverhog-ftp-spool-client:a-riverhog-ftp-spool-client-riverhogftpsp-cc81b4f744:e02f6d0d56 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-ftp-spool-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-3beeecde40"></a>
- <a id="s-384a7804f9"></a>`distribution`: `a-riverhog-ftp-spool-client`
- <a id="s-704b101b2d"></a>`module`: `a_riverhog_ftp_spool_client`
- <a id="s-0cef1eb974"></a>`name`: `ftp_spool_health_ready`
- <a id="s-f876791471"></a>`owner`: `a_riverhog_ftp_spool_client.RiverhogFtpSpoolClient`
- <a id="s-65770a0a6a"></a>`unit`: `member`

### Declared structure

- <a id="s-f6685ab252"></a>`kind`: `"method"`
- <a id="s-39d93b6e27"></a>`signature`: `"\"(self) -> 'HealthOut'\""`

## Maintained corroboration

### Related interface records

- [GET /health/ready](../../a-riverhog-ftp-spool/http-operations/get-health-ready.md)
- [RiverhogFtpSpoolClient](a-riverhog-ftp-spool-client-riverhogftpspoolclient.md)

## Governing policies

- <a id="pa-1f7ceed6d8"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:a-riverhog-ftp-spool-client:a_riverhog_ftp_spool_client](../../../evidence/sources/authorities.md#src-3caa0330b6) — [some-implementations/riverhog/ingress/ftp-api-client/src/a\_riverhog\_ftp\_spool\_client/\_\_init\_\_.py](../../../../../../some-implementations/riverhog/ingress/ftp-api-client/src/a_riverhog_ftp_spool_client/__init__.py)
- **Client method:** [some-implementations/riverhog/ingress/ftp-api-client/src/a\_riverhog\_ftp\_spool\_client/client.py::RiverhogFtpSpoolClient.ftp\_spool\_health\_ready](../../../../../../some-implementations/riverhog/ingress/ftp-api-client/src/a_riverhog_ftp_spool_client/client.py#L86)

### Machine authority

- `/external_contract/python/a_riverhog_ftp_spool_client.RiverhogFtpSpoolClient.ftp_spool_health_ready`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 4c6eb182ca831e5c270c0bc2a4b77f3701832768e37deba520b40b893ec5b2e8 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'HealthOut'\""
  },
  "distribution": "a-riverhog-ftp-spool-client",
  "module": "a_riverhog_ftp_spool_client",
  "name": "ftp_spool_health_ready",
  "owner": "a_riverhog_ftp_spool_client.RiverhogFtpSpoolClient",
  "unit": "member"
}
```

</details>
