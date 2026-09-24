# riverhog_client.ApiClient.replace_app_key_access

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-apiclient-replace-app-key-access:199477ede1 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-ee7f16fca9"></a>
- <a id="s-7fa65928d4"></a>`distribution`: `riverhog-client`
- <a id="s-65d9629cff"></a>`module`: `riverhog_client`
- <a id="s-425a76a5a9"></a>`name`: `replace_app_key_access`
- <a id="s-48b4009295"></a>`owner`: `riverhog_client.ApiClient`
- <a id="s-18148ac8ca"></a>`unit`: `member`

### Declared structure

- <a id="s-549ec6e31b"></a>`kind`: `"method"`
- <a id="s-f027e65a56"></a>`signature`: `"\"(self, app: 'ApplicationName', key_id: 'ApplicationKeyId', *, access: 'Sequence[Mapping[str, str]]') -> 'dict[str, Any]'\""`

## Maintained corroboration

### Related interface records

- [a-riverhog-cli app key access set](../../a-riverhog-cli/cli/a-riverhog-cli-app-key-access-set.md)
- [PUT /v1/apps/{app}/keys/{key_id}/access](../../riverhog/http-operations/put-v1-apps-app-keys-key-id-access.md)
- [ApiClient](riverhog-client-apiclient.md)

## Governing policies

- <a id="pa-ea62aec75a"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-client:riverhog_client](../../../evidence/sources/authorities.md#src-c149020c71) — [packages/riverhog-client/src/riverhog\_client/\_\_init\_\_.py](../../../../../../packages/riverhog-client/src/riverhog_client/__init__.py)
- **Client method:** [packages/riverhog-client/src/riverhog\_client/client.py::ApiClient.replace\_app\_key\_access](../../../../../../packages/riverhog-client/src/riverhog_client/client.py#L2217)

### Machine authority

- `/external_contract/python/riverhog_client.ApiClient.replace_app_key_access`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: ebd64c0c7703d270fde96d3af0355223a73b15cabc2efd8208ce2e67bd410614 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, app: 'ApplicationName', key_id: 'ApplicationKeyId', *, access: 'Sequence[Mapping[str, str]]') -> 'dict[str, Any]'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client",
  "name": "replace_app_key_access",
  "owner": "riverhog_client.ApiClient",
  "unit": "member"
}
```

</details>
