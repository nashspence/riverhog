# riverhog_client.ApiClient.get_collection

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-apiclient-get-collection:594dd21f6c -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-139481444d"></a>
- <a id="s-a8c73fc8d7"></a>`distribution`: `riverhog-client`
- <a id="s-acb6caa872"></a>`module`: `riverhog_client`
- <a id="s-9c611be313"></a>`name`: `get_collection`
- <a id="s-912c42f976"></a>`owner`: `riverhog_client.ApiClient`
- <a id="s-19708e57a1"></a>`unit`: `member`

### Declared structure

- <a id="s-bd16934e50"></a>`kind`: `"method"`
- <a id="s-5151962592"></a>`signature`: `"\"(self, collection_id: 'CollectionId') -> 'dict[str, Any]'\""`

## Maintained corroboration

### Related interface records

- [a-riverhog-cli collection describe](../../a-riverhog-cli/cli/a-riverhog-cli-collection-describe.md)
- [a-riverhog-cli collection show](../../a-riverhog-cli/cli/a-riverhog-cli-collection-show.md)
- [a-riverhog-cli collection tag add](../../a-riverhog-cli/cli/a-riverhog-cli-collection-tag-add.md)
- [a-riverhog-cli collection tag contains](../../a-riverhog-cli/cli/a-riverhog-cli-collection-tag-contains.md)
- [a-riverhog-cli collection tag list](../../a-riverhog-cli/cli/a-riverhog-cli-collection-tag-list.md)
- [a-riverhog-cli collection tag remove](../../a-riverhog-cli/cli/a-riverhog-cli-collection-tag-remove.md)
- [a-riverhog-cli local add](../../a-riverhog-cli/cli/a-riverhog-cli-local-add.md)
- [a-riverhog-cli local repair](../../a-riverhog-cli/cli/a-riverhog-cli-local-repair.md)
- [a-riverhog-cli local sync](../../a-riverhog-cli/cli/a-riverhog-cli-local-sync.md)
- [GET /v1/collections/{collection_id}](../../riverhog/http-operations/get-v1-collections-collection-id.md)
- [ApiClient](riverhog-client-apiclient.md)

## Governing policies

- <a id="pa-4df2fea9b0"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-client:riverhog_client](../../../evidence/sources/authorities.md#src-c149020c71) — [packages/riverhog-client/src/riverhog\_client/\_\_init\_\_.py](../../../../../../packages/riverhog-client/src/riverhog_client/__init__.py)
- **Client method:** [packages/riverhog-client/src/riverhog\_client/client.py::ApiClient.get\_collection](../../../../../../packages/riverhog-client/src/riverhog_client/client.py#L1583)

### Machine authority

- `/external_contract/python/riverhog_client.ApiClient.get_collection`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 63caa80c2e75165ff4cb572ece2481086c6b03a520cd6c0bcba88e696d74bf08 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, collection_id: 'CollectionId') -> 'dict[str, Any]'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client",
  "name": "get_collection",
  "owner": "riverhog_client.ApiClient",
  "unit": "member"
}
```

</details>
