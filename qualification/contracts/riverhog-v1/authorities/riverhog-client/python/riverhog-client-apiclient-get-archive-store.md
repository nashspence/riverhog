# riverhog_client.ApiClient.get_archive_store

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-apiclient-get-archive-store:f4b8f41bec -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-ddcb962352"></a>
- <a id="s-d3bd8ff95e"></a>`distribution`: `riverhog-client`
- <a id="s-7d450b9489"></a>`module`: `riverhog_client`
- <a id="s-31f9a4a020"></a>`name`: `get_archive_store`
- <a id="s-9ead976a4f"></a>`owner`: `riverhog_client.ApiClient`
- <a id="s-8bf05c83ac"></a>`unit`: `member`

### Declared structure

- <a id="s-7a8e49e0aa"></a>`kind`: `"method"`
- <a id="s-f6c8bf1b42"></a>`signature`: `"\"(self, store: 'ArchiveStoreName') -> 'dict[str, Any]'\""`

## Maintained corroboration

### Related interface records

- [piggity archive store show](../../piggity/cli/piggity-archive-store-show.md)
- [GET /v1/archive/stores/{store}](../../riverhog/http-operations/get-v1-archive-stores-store.md)
- [ApiClient](riverhog-client-apiclient.md)

## Governing policies

- <a id="pa-ef5e8933a4"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-client:riverhog_client](../../../evidence/sources/authorities.md#src-c149020c71) — [packages/riverhog-client/src/riverhog\_client/\_\_init\_\_.py](../../../../../../packages/riverhog-client/src/riverhog_client/__init__.py)
- **Client method:** [packages/riverhog-client/src/riverhog\_client/client.py::ApiClient.get\_archive\_store](../../../../../../packages/riverhog-client/src/riverhog_client/client.py#L2067)

### Machine authority

- `/external_contract/python/riverhog_client.ApiClient.get_archive_store`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: b6a31d6eb9e9a93dc84a8c61158cbc1e5d5081dfbc978c6ef82ecc9d5b224c9e -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, store: 'ArchiveStoreName') -> 'dict[str, Any]'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client",
  "name": "get_archive_store",
  "owner": "riverhog_client.ApiClient",
  "unit": "member"
}
```

</details>
