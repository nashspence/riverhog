# riverhog_client.ApiClient.get_download_quota

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-apiclient-get-download-quota:ce37bb8e29 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-0d9390a844"></a>
- <a id="s-9453bc2316"></a>`distribution`: `riverhog-client`
- <a id="s-70e7327622"></a>`module`: `riverhog_client`
- <a id="s-4f788711b8"></a>`name`: `get_download_quota`
- <a id="s-efad52c226"></a>`owner`: `riverhog_client.ApiClient`
- <a id="s-d59c1287bb"></a>`unit`: `member`

### Declared structure

- <a id="s-68d1abd3b5"></a>`kind`: `"method"`
- <a id="s-7178761e79"></a>`signature`: `"\"(self) -> 'dict[str, Any]'\""`

## Maintained corroboration

### Related interface records

- [riverhog_client.ApiClient](riverhog-client-apiclient.md)

## Governing policies

- <a id="pa-0d95b5de00"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-client:riverhog_client](../../../evidence/sources.md#src-c149020c71) — `packages/riverhog-client/src/riverhog_client/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_client.ApiClient.get_download_quota`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: f1ec122fb2d853babc6e8d2a6f4e16c4e8f534c51e34a127f77d2a371637b496 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'dict[str, Any]'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client",
  "name": "get_download_quota",
  "owner": "riverhog_client.ApiClient",
  "unit": "member"
}
```
