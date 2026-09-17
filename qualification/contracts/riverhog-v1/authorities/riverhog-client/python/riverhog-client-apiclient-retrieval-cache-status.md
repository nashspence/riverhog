# riverhog_client.ApiClient.retrieval_cache_status

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-apiclient-retrieval-cache-status:a573761a46 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-1e52671242"></a>
- <a id="s-5d4313cca3"></a>`distribution`: `riverhog-client`
- <a id="s-aaa0e875a4"></a>`module`: `riverhog_client`
- <a id="s-abee46b1d5"></a>`name`: `retrieval_cache_status`
- <a id="s-40f6aedb55"></a>`owner`: `riverhog_client.ApiClient`
- <a id="s-657bdb0ea2"></a>`unit`: `member`

### Declared structure

- <a id="s-83647273ad"></a>`kind`: `"method"`
- <a id="s-de16f63b56"></a>`signature`: `"\"(self) -> 'dict[str, Any]'\""`

## Maintained corroboration

### Related interface records

- [piggity retrieval cache status](../../piggity/cli/piggity-retrieval-cache-status.md)
- [GET /v1/retrieval-cache](../../riverhog/http-operations/get-v1-retrieval-cache.md)
- [ApiClient](riverhog-client-apiclient.md)

## Governing policies

- <a id="pa-3597b5179a"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-client:riverhog_client](../../../evidence/sources.md#src-c149020c71) — [packages/riverhog-client/src/riverhog\_client/\_\_init\_\_.py](../../../../../../packages/riverhog-client/src/riverhog_client/__init__.py)
- **Client method:** [packages/riverhog-client/src/riverhog\_client/client.py::ApiClient.retrieval\_cache\_status](../../../../../../packages/riverhog-client/src/riverhog_client/client.py#L898)

### Machine authority

- `/external_contract/python/riverhog_client.ApiClient.retrieval_cache_status`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 9deba9944601c690c13c1be1e3f1803fa66c8ab9f31c03457c82f638950a7d7c -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'dict[str, Any]'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client",
  "name": "retrieval_cache_status",
  "owner": "riverhog_client.ApiClient",
  "unit": "member"
}
```

</details>
