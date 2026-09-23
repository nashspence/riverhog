# a_riverhog_ftp_spool_client.RiverhogFtpSpoolClient.__exit__

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:a-riverhog-ftp-spool-client:a-riverhog-ftp-spool-client-riverhogftpsp-ae6d2cdc45:a39ad8168c -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-ftp-spool-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-2bbab8bd42"></a>
- <a id="s-a46826c693"></a>`distribution`: `a-riverhog-ftp-spool-client`
- <a id="s-4e7bf8b148"></a>`module`: `a_riverhog_ftp_spool_client`
- <a id="s-90ea7e1a0c"></a>`name`: `__exit__`
- <a id="s-afb46d5745"></a>`owner`: `a_riverhog_ftp_spool_client.RiverhogFtpSpoolClient`
- <a id="s-ccdb07f15e"></a>`unit`: `member`

### Declared structure

- <a id="s-064aae1b1a"></a>`kind`: `"method"`
- <a id="s-f4c148e12c"></a>`signature`: `"\"(self, *_exc: 'object') -> 'None'\""`

## Maintained corroboration

### Related interface records

- [RiverhogFtpSpoolClient](a-riverhog-ftp-spool-client-riverhogftpspoolclient.md)

## Governing policies

- <a id="pa-eb621a1ede"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:a-riverhog-ftp-spool-client:a_riverhog_ftp_spool_client](../../../evidence/sources/authorities.md#src-3caa0330b6) — [some-implementations/riverhog/ingress/ftp-api-client/src/a\_riverhog\_ftp\_spool\_client/\_\_init\_\_.py](../../../../../../some-implementations/riverhog/ingress/ftp-api-client/src/a_riverhog_ftp_spool_client/__init__.py)

### Machine authority

- `/external_contract/python/a_riverhog_ftp_spool_client.RiverhogFtpSpoolClient.__exit__`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: a3b501d22bddf066e8e6057058add50564c9fc32689757a1c99f13e75221979e -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, *_exc: 'object') -> 'None'\""
  },
  "distribution": "a-riverhog-ftp-spool-client",
  "module": "a_riverhog_ftp_spool_client",
  "name": "__exit__",
  "owner": "a_riverhog_ftp_spool_client.RiverhogFtpSpoolClient",
  "unit": "member"
}
```

</details>
