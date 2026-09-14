# riverhog_ftp_adapter.FtpAdapterConfig.provenance_authority

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-ftp-adapter:riverhog-ftp-adapter-ftpadapterconfig-pro-e081c8f132:6a06d9eaef -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-ftp-adapter](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-09178207d8"></a>
- <a id="s-d931c5b181"></a>`distribution`: `riverhog-ftp-adapter`
- <a id="s-9e63fb8372"></a>`module`: `riverhog_ftp_adapter`
- <a id="s-110366711b"></a>`name`: `provenance_authority`
- <a id="s-c8457d9590"></a>`owner`: `riverhog_ftp_adapter.FtpAdapterConfig`
- <a id="s-a61cea1421"></a>`unit`: `member`

### Declared structure

- <a id="s-7f19825276"></a>`kind`: `"method"`
- <a id="s-55af4d2805"></a>`signature`: `"\"(self) -> 'Self'\""`

## Maintained corroboration

### Related interface records

- [FtpAdapterConfig](riverhog-ftp-adapter-ftpadapterconfig.md)

## Governing policies

- <a id="pa-2614644ba1"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-ftp-adapter:riverhog_ftp_adapter](../../../evidence/sources.md#src-8d11f8fa97) — `reference/riverhog/ingress/ftp/src/riverhog_ftp_adapter/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_ftp_adapter.FtpAdapterConfig.provenance_authority`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: d506b161a111b67cd9f7553f01952795181762a66a70790094f0387464eae917 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'Self'\""
  },
  "distribution": "riverhog-ftp-adapter",
  "module": "riverhog_ftp_adapter",
  "name": "provenance_authority",
  "owner": "riverhog_ftp_adapter.FtpAdapterConfig",
  "unit": "member"
}
```
