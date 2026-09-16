# riverhog_client.ApiClient.get_collection

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-apiclient-get-collection:594dd21f6c -->

Exact externally visible contract owned by this semantic dossier.

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

- [piggity collection describe](../../piggity/cli/piggity-collection-describe.md)
- [piggity collection show](../../piggity/cli/piggity-collection-show.md)
- [piggity collection tag add](../../piggity/cli/piggity-collection-tag-add.md)
- [piggity collection tag contains](../../piggity/cli/piggity-collection-tag-contains.md)
- [piggity collection tag list](../../piggity/cli/piggity-collection-tag-list.md)
- [piggity collection tag remove](../../piggity/cli/piggity-collection-tag-remove.md)
- [piggity local add](../../piggity/cli/piggity-local-add.md)
- [piggity local repair](../../piggity/cli/piggity-local-repair.md)
- [piggity local sync](../../piggity/cli/piggity-local-sync.md)
- [GET /v1/collections/{collection_id}](../../riverhog/http-operations/get-v1-collections-collection-id.md)
- [ApiClient](riverhog-client-apiclient.md)

## Governing policies

- <a id="pa-4df2fea9b0"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-client:riverhog_client](../../../evidence/sources.md#src-c149020c71) — `packages/riverhog-client/src/riverhog_client/__init__.py`
- **Client method:** [packages/riverhog-client/src/riverhog_client/client.py::ApiClient.get_collection](../../../../../../packages/riverhog-client/src/riverhog_client/client.py#L1572)

### Machine authority

- `/external_contract/python/riverhog_client.ApiClient.get_collection`

### Exact owned JSON

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
