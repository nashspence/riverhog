# riverhog_client.ApiClient.add_app_key_access

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-apiclient-add-app-key-access:7db2a71e28 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-df3da72b40"></a>
- <a id="s-dc19011904"></a>`distribution`: `riverhog-client`
- <a id="s-976cdd224c"></a>`module`: `riverhog_client`
- <a id="s-4604b113c9"></a>`name`: `add_app_key_access`
- <a id="s-545a8a886f"></a>`owner`: `riverhog_client.ApiClient`
- <a id="s-7abb5b4c6d"></a>`unit`: `member`

### Declared structure

- <a id="s-8bc54ba444"></a>`kind`: `"method"`
- <a id="s-80b2e49da5"></a>`signature`: `"\"(self, app: 'ApplicationName', key_id: 'ApplicationKeyId', *, permission: 'ApplicationPermission', resource: 'ApplicationResource') -> 'dict[str, Any]'\""`

## Maintained corroboration

### Related interface records

- [piggity app key access add](../../piggity/cli/piggity-app-key-access-add.md)
- [POST /v1/apps/{app}/keys/{key_id}/access](../../riverhog/http-operations/post-v1-apps-app-keys-key-id-access.md)
- [ApiClient](riverhog-client-apiclient.md)

## Governing policies

- <a id="pa-6b456c4307"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-client:riverhog_client](../../../evidence/sources.md#src-c149020c71) — [packages/riverhog-client/src/riverhog\_client/\_\_init\_\_.py](../../../../../../packages/riverhog-client/src/riverhog_client/__init__.py)
- **Client method:** [packages/riverhog-client/src/riverhog\_client/client.py::ApiClient.add\_app\_key\_access](../../../../../../packages/riverhog-client/src/riverhog_client/client.py#L2200)

### Machine authority

- `/external_contract/python/riverhog_client.ApiClient.add_app_key_access`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 9685944ba9a4107726374462bba17c3fdb0958aca0b1956228cadaef8a93e851 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, app: 'ApplicationName', key_id: 'ApplicationKeyId', *, permission: 'ApplicationPermission', resource: 'ApplicationResource') -> 'dict[str, Any]'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client",
  "name": "add_app_key_access",
  "owner": "riverhog_client.ApiClient",
  "unit": "member"
}
```

</details>
