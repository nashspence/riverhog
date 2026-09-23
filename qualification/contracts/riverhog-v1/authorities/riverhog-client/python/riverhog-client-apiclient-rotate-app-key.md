# riverhog_client.ApiClient.rotate_app_key

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-apiclient-rotate-app-key:5ac2f3af9f -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-b3d43ce565"></a>
- <a id="s-0e03082e92"></a>`distribution`: `riverhog-client`
- <a id="s-f81901cf9f"></a>`module`: `riverhog_client`
- <a id="s-3029ee3e70"></a>`name`: `rotate_app_key`
- <a id="s-22d4d2386b"></a>`owner`: `riverhog_client.ApiClient`
- <a id="s-3fbcac377b"></a>`unit`: `member`

### Declared structure

- <a id="s-d5d0176ab4"></a>`kind`: `"method"`
- <a id="s-27f84a9f26"></a>`signature`: `"\"(self, app: 'ApplicationName', key_id: 'ApplicationKeyId') -> 'dict[str, Any]'\""`

## Maintained corroboration

### Related interface records

- [piggity app key rotate](../../piggity/cli/piggity-app-key-rotate.md)
- [POST /v1/apps/{app}/keys/{key_id}/rotate](../../riverhog/http-operations/post-v1-apps-app-keys-key-id-rotate.md)
- [ApiClient](riverhog-client-apiclient.md)

## Governing policies

- <a id="pa-e094125403"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-client:riverhog_client](../../../evidence/sources/authorities.md#src-c149020c71) — [packages/riverhog-client/src/riverhog\_client/\_\_init\_\_.py](../../../../../../packages/riverhog-client/src/riverhog_client/__init__.py)
- **Client method:** [packages/riverhog-client/src/riverhog\_client/client.py::ApiClient.rotate\_app\_key](../../../../../../packages/riverhog-client/src/riverhog_client/client.py#L2146)

### Machine authority

- `/external_contract/python/riverhog_client.ApiClient.rotate_app_key`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: fbcbf5584efd8d3d3c416a5966963f3da3a2a5963cceefb8368ebf595dd30ea0 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, app: 'ApplicationName', key_id: 'ApplicationKeyId') -> 'dict[str, Any]'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client",
  "name": "rotate_app_key",
  "owner": "riverhog_client.ApiClient",
  "unit": "member"
}
```

</details>
