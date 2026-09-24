# riverhog_client.ApiClient.list_tags

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-apiclient-list-tags:3aa77a2a5a -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-fed1e8aaf3"></a>
- <a id="s-92e8ce2783"></a>`distribution`: `riverhog-client`
- <a id="s-d40f077218"></a>`module`: `riverhog_client`
- <a id="s-7a53ea0f47"></a>`name`: `list_tags`
- <a id="s-567bc70c89"></a>`owner`: `riverhog_client.ApiClient`
- <a id="s-9693eb54d8"></a>`unit`: `member`

### Declared structure

- <a id="s-588d3c953c"></a>`kind`: `"method"`
- <a id="s-15027e1462"></a>`signature`: `"\"(self, *, page_size: 'int' = 25, page_token: 'str \| None' = None, q: 'str \| None' = None) -> 'dict[str, Any]'\""`

## Maintained corroboration

### Related interface records

- [a-riverhog-cli tag list](../../a-riverhog-cli/cli/a-riverhog-cli-tag-list.md)
- [GET /v1/tags](../../riverhog/http-operations/get-v1-tags.md)
- [ApiClient](riverhog-client-apiclient.md)

## Governing policies

- <a id="pa-4a96473e3c"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-client:riverhog_client](../../../evidence/sources/authorities.md#src-c149020c71) — [packages/riverhog-client/src/riverhog\_client/\_\_init\_\_.py](../../../../../../packages/riverhog-client/src/riverhog_client/__init__.py)
- **Client method:** [packages/riverhog-client/src/riverhog\_client/client.py::ApiClient.list\_tags](../../../../../../packages/riverhog-client/src/riverhog_client/client.py#L2253)

### Machine authority

- `/external_contract/python/riverhog_client.ApiClient.list_tags`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 1ed78e63b19387e5beba34e2afafc3f174dad0458fb2c572585935599314aec8 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, *, page_size: 'int' = 25, page_token: 'str | None' = None, q: 'str | None' = None) -> 'dict[str, Any]'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client",
  "name": "list_tags",
  "owner": "riverhog_client.ApiClient",
  "unit": "member"
}
```

</details>
