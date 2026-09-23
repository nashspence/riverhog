# a_riverhog_ftp_spool_client.RiverhogFtpSpoolClient.ftp_spool_health_live

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:a-riverhog-ftp-spool-client:a-riverhog-ftp-spool-client-riverhogftpsp-0c7456b718:059aa0f9d4 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-ftp-spool-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-493087dc50"></a>
- <a id="s-e8de5e2236"></a>`distribution`: `a-riverhog-ftp-spool-client`
- <a id="s-35cac82476"></a>`module`: `a_riverhog_ftp_spool_client`
- <a id="s-f741b13d00"></a>`name`: `ftp_spool_health_live`
- <a id="s-4a5c26a3c0"></a>`owner`: `a_riverhog_ftp_spool_client.RiverhogFtpSpoolClient`
- <a id="s-45314726c5"></a>`unit`: `member`

### Declared structure

- <a id="s-ab9e7a4543"></a>`kind`: `"method"`
- <a id="s-3764ccfa06"></a>`signature`: `"\"(self) -> 'HealthResponse'\""`

## Maintained corroboration

### Related interface records

- [GET /health/live](../../a-riverhog-ftp-spool/http-operations/get-health-live.md)
- [RiverhogFtpSpoolClient](a-riverhog-ftp-spool-client-riverhogftpspoolclient.md)

## Governing policies

- <a id="pa-ccac5e0ce7"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:a-riverhog-ftp-spool-client:a_riverhog_ftp_spool_client](../../../evidence/sources/authorities.md#src-3caa0330b6) — [some-implementations/riverhog/ingress/ftp-api-client/src/a\_riverhog\_ftp\_spool\_client/\_\_init\_\_.py](../../../../../../some-implementations/riverhog/ingress/ftp-api-client/src/a_riverhog_ftp_spool_client/__init__.py)
- **Client method:** [some-implementations/riverhog/ingress/ftp-api-client/src/a\_riverhog\_ftp\_spool\_client/client.py::RiverhogFtpSpoolClient.ftp\_spool\_health\_live](../../../../../../some-implementations/riverhog/ingress/ftp-api-client/src/a_riverhog_ftp_spool_client/client.py#L80)

### Machine authority

- `/external_contract/python/a_riverhog_ftp_spool_client.RiverhogFtpSpoolClient.ftp_spool_health_live`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 9613315dc062e2e21aa5a6fb81af000917c8cdc79a015b16e3731a3f5d8bac7e -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'HealthResponse'\""
  },
  "distribution": "a-riverhog-ftp-spool-client",
  "module": "a_riverhog_ftp_spool_client",
  "name": "ftp_spool_health_live",
  "owner": "a_riverhog_ftp_spool_client.RiverhogFtpSpoolClient",
  "unit": "member"
}
```

</details>
