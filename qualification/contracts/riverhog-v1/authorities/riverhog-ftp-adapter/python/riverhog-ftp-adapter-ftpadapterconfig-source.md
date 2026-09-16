# riverhog_ftp_adapter.FtpAdapterConfig.source

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-ftp-adapter:riverhog-ftp-adapter-ftpadapterconfig-source:ac5ad1d957 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-ftp-adapter](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-63c31123bc"></a>
- <a id="s-abbe5689f3"></a>`distribution`: `riverhog-ftp-adapter`
- <a id="s-637d99f2e4"></a>`module`: `riverhog_ftp_adapter`
- <a id="s-713666d891"></a>`name`: `source`
- <a id="s-00d77f27f7"></a>`owner`: `riverhog_ftp_adapter.FtpAdapterConfig`
- <a id="s-a829cffd02"></a>`unit`: `member`

### Declared structure

- <a id="s-01b11aa7d3"></a>`kind`: `"method"`
- <a id="s-98cb7b721f"></a>`signature`: `"\"(self, source_id: 'str') -> 'SourceConfig'\""`

## Maintained corroboration

### Related interface records

- [FtpAdapterConfig](riverhog-ftp-adapter-ftpadapterconfig.md)

## Governing policies

- <a id="pa-f26b761995"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-ftp-adapter:riverhog_ftp_adapter](../../../evidence/sources.md#src-8d11f8fa97) — `reference/riverhog/ingress/ftp/src/riverhog_ftp_adapter/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_ftp_adapter.FtpAdapterConfig.source`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 7a85b92fd89a341bc3399a3e8d4960a347282c13ccc3e9caee63d16640ef89e9 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, source_id: 'str') -> 'SourceConfig'\""
  },
  "distribution": "riverhog-ftp-adapter",
  "module": "riverhog_ftp_adapter",
  "name": "source",
  "owner": "riverhog_ftp_adapter.FtpAdapterConfig",
  "unit": "member"
}
```

</details>
