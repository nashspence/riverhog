# riverhog_client.ApiClient.remove_app_key_access

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-apiclient-remove-app-key-access:1e61b7f3b7 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-60965284ce"></a>
- <a id="s-76f9379033"></a>`distribution`: `riverhog-client`
- <a id="s-830d05130b"></a>`module`: `riverhog_client`
- <a id="s-b01e637ffd"></a>`name`: `remove_app_key_access`
- <a id="s-d3d2c70008"></a>`owner`: `riverhog_client.ApiClient`
- <a id="s-a324f4b686"></a>`unit`: `member`

### Declared structure

- <a id="s-c9ef3338b7"></a>`kind`: `"method"`
- <a id="s-7f69b05149"></a>`signature`: `"\"(self, app: 'ApplicationName', key_id: 'ApplicationKeyId', *, permission: 'ApplicationPermission', resource: 'ApplicationResource') -> 'dict[str, Any]'\""`

## Maintained corroboration

### Related interface records

- [piggity app key access remove](../../piggity/cli/piggity-app-key-access-remove.md)
- [DELETE /v1/apps/{app}/keys/{key_id}/access](../../riverhog/http-operations/delete-v1-apps-app-keys-key-id-access.md)
- [ApiClient](riverhog-client-apiclient.md)

## Governing policies

- <a id="pa-fa9de82077"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-client:riverhog_client](../../../evidence/sources.md#src-c149020c71) — [packages/riverhog-client/src/riverhog\_client/\_\_init\_\_.py](../../../../../../packages/riverhog-client/src/riverhog_client/__init__.py)
- **Client method:** [packages/riverhog-client/src/riverhog\_client/client.py::ApiClient.remove\_app\_key\_access](../../../../../../packages/riverhog-client/src/riverhog_client/client.py#L2216)

### Machine authority

- `/external_contract/python/riverhog_client.ApiClient.remove_app_key_access`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: b6e7c26c70510a8a5789580ef54212695c7e2f2635fdccdff7106e5d1e661d5d -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, app: 'ApplicationName', key_id: 'ApplicationKeyId', *, permission: 'ApplicationPermission', resource: 'ApplicationResource') -> 'dict[str, Any]'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client",
  "name": "remove_app_key_access",
  "owner": "riverhog_client.ApiClient",
  "unit": "member"
}
```

</details>
