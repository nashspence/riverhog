# riverhog_ftp_adapter.FtpAdapterConfig.unique_sources

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-ftp-adapter:riverhog-ftp-adapter-ftpadapterconfig-unique-sources:ac3ce092e5 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-ftp-adapter](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-f60fee0c54"></a>
- <a id="s-f0fd4589e4"></a>`distribution`: `riverhog-ftp-adapter`
- <a id="s-5b28692da7"></a>`module`: `riverhog_ftp_adapter`
- <a id="s-6f86ba0851"></a>`name`: `unique_sources`
- <a id="s-2a8be80b35"></a>`owner`: `riverhog_ftp_adapter.FtpAdapterConfig`
- <a id="s-94938b57ec"></a>`unit`: `member`

### Declared structure

- <a id="s-b24f0118ff"></a>`kind`: `"classmethod"`
- <a id="s-b9871af2a2"></a>`signature`: `"\"(cls, value: 'tuple[SourceConfig, ...]') -> 'tuple[SourceConfig, ...]'\""`

## Maintained corroboration

### Related interface records

- [FtpAdapterConfig](riverhog-ftp-adapter-ftpadapterconfig.md)

## Governing policies

- <a id="pa-2dfa0e3ade"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-ftp-adapter:riverhog_ftp_adapter](../../../evidence/sources.md#src-8d11f8fa97) — `reference/riverhog/ingress/ftp/src/riverhog_ftp_adapter/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_ftp_adapter.FtpAdapterConfig.unique_sources`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 9814be332f649b780c43b86c3f36bd5b18be9694301c10f802860cb4ed7d35c2 -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, value: 'tuple[SourceConfig, ...]') -> 'tuple[SourceConfig, ...]'\""
  },
  "distribution": "riverhog-ftp-adapter",
  "module": "riverhog_ftp_adapter",
  "name": "unique_sources",
  "owner": "riverhog_ftp_adapter.FtpAdapterConfig",
  "unit": "member"
}
```
