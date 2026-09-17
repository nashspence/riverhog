# riverhog_client.ApiClient.revoke_app_key

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-apiclient-revoke-app-key:0bd07fd14f -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-d8e11c1ae7"></a>
- <a id="s-7d275cebb1"></a>`distribution`: `riverhog-client`
- <a id="s-1f3a504bf1"></a>`module`: `riverhog_client`
- <a id="s-073d67e59c"></a>`name`: `revoke_app_key`
- <a id="s-6125ed2fe1"></a>`owner`: `riverhog_client.ApiClient`
- <a id="s-9be687366d"></a>`unit`: `member`

### Declared structure

- <a id="s-c69c83f43b"></a>`kind`: `"method"`
- <a id="s-9af89149aa"></a>`signature`: `"\"(self, app: 'ApplicationName', key_id: 'ApplicationKeyId') -> 'dict[str, Any]'\""`

## Maintained corroboration

### Related interface records

- [piggity app key revoke](../../piggity/cli/piggity-app-key-revoke.md)
- [POST /v1/apps/{app}/keys/{key_id}/revoke](../../riverhog/http-operations/post-v1-apps-app-keys-key-id-revoke.md)
- [ApiClient](riverhog-client-apiclient.md)

## Governing policies

- <a id="pa-b58b0212fa"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-client:riverhog_client](../../../evidence/sources/authorities.md#src-c149020c71) — [packages/riverhog-client/src/riverhog\_client/\_\_init\_\_.py](../../../../../../packages/riverhog-client/src/riverhog_client/__init__.py)
- **Client method:** [packages/riverhog-client/src/riverhog\_client/client.py::ApiClient.revoke\_app\_key](../../../../../../packages/riverhog-client/src/riverhog_client/client.py#L2132)

### Machine authority

- `/external_contract/python/riverhog_client.ApiClient.revoke_app_key`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: ce6107576c82ed95caae43d8a3790ba35fd3427cfce0b460b36544a50de59d6c -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, app: 'ApplicationName', key_id: 'ApplicationKeyId') -> 'dict[str, Any]'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client",
  "name": "revoke_app_key",
  "owner": "riverhog_client.ApiClient",
  "unit": "member"
}
```

</details>
