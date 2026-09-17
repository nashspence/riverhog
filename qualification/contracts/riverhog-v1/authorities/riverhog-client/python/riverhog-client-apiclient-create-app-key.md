# riverhog_client.ApiClient.create_app_key

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-apiclient-create-app-key:8fc4cca262 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-a6ed37379c"></a>
- <a id="s-68b67127e9"></a>`distribution`: `riverhog-client`
- <a id="s-ba2371d0bc"></a>`module`: `riverhog_client`
- <a id="s-9b3c20ca38"></a>`name`: `create_app_key`
- <a id="s-848357a4d6"></a>`owner`: `riverhog_client.ApiClient`
- <a id="s-f5264611a6"></a>`unit`: `member`

### Declared structure

- <a id="s-ce328d90af"></a>`kind`: `"method"`
- <a id="s-46afd4b393"></a>`signature`: `"\"(self, app: 'ApplicationName', *, access: 'Sequence[Mapping[str, str]]', expires_in_seconds: 'int \| None' = None) -> 'dict[str, Any]'\""`

## Maintained corroboration

### Related interface records

- [piggity app key create](../../piggity/cli/piggity-app-key-create.md)
- [POST /v1/apps/{app}/keys](../../riverhog/http-operations/post-v1-apps-app-keys.md)
- [ApiClient](riverhog-client-apiclient.md)

## Governing policies

- <a id="pa-98cfb3d850"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-client:riverhog_client](../../../evidence/sources/authorities.md#src-c149020c71) — [packages/riverhog-client/src/riverhog\_client/\_\_init\_\_.py](../../../../../../packages/riverhog-client/src/riverhog_client/__init__.py)
- **Client method:** [packages/riverhog-client/src/riverhog\_client/client.py::ApiClient.create\_app\_key](../../../../../../packages/riverhog-client/src/riverhog_client/client.py#L2084)

### Machine authority

- `/external_contract/python/riverhog_client.ApiClient.create_app_key`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 3f3433b1dcb062772e4474d95740cd9bb14aea4586d0b15473dcee7c3b5d107a -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, app: 'ApplicationName', *, access: 'Sequence[Mapping[str, str]]', expires_in_seconds: 'int | None' = None) -> 'dict[str, Any]'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client",
  "name": "create_app_key",
  "owner": "riverhog_client.ApiClient",
  "unit": "member"
}
```

</details>
