# riverhog_client.ApiClient.create_retrieval_job

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-apiclient-create-retrieval-job:a0cd0fd29c -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-d4fc88f45d"></a>
- <a id="s-1c10d062e3"></a>`distribution`: `riverhog-client`
- <a id="s-c8a0f590d8"></a>`module`: `riverhog_client`
- <a id="s-4291521bf9"></a>`name`: `create_retrieval_job`
- <a id="s-f94ec4ba29"></a>`owner`: `riverhog_client.ApiClient`
- <a id="s-bd92602ac4"></a>`unit`: `member`

### Declared structure

- <a id="s-a0e3c0814f"></a>`kind`: `"method"`
- <a id="s-d496708a65"></a>`signature`: `"\"(self, plan_id: 'str', *, plan_etag: 'str', event_context: 'Mapping[str, Any] \| None' = None) -> 'dict[str, Any]'\""`

## Maintained corroboration

### Related interface records

- [piggity local repair](../../piggity/cli/piggity-local-repair.md)
- [piggity local sync](../../piggity/cli/piggity-local-sync.md)
- [POST /v1/retrieval-jobs](../../riverhog/http-operations/post-v1-retrieval-jobs.md)
- [ApiClient](riverhog-client-apiclient.md)

## Governing policies

- <a id="pa-827141e222"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-client:riverhog_client](../../../evidence/sources/authorities.md#src-c149020c71) — [packages/riverhog-client/src/riverhog\_client/\_\_init\_\_.py](../../../../../../packages/riverhog-client/src/riverhog_client/__init__.py)
- **Client method:** [packages/riverhog-client/src/riverhog\_client/client.py::ApiClient.create\_retrieval\_job](../../../../../../packages/riverhog-client/src/riverhog_client/client.py#L857)

### Machine authority

- `/external_contract/python/riverhog_client.ApiClient.create_retrieval_job`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: f61643cf0098991a923595e388ff2264ce9b5c32719575711c960cab749ade83 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, plan_id: 'str', *, plan_etag: 'str', event_context: 'Mapping[str, Any] | None' = None) -> 'dict[str, Any]'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client",
  "name": "create_retrieval_job",
  "owner": "riverhog_client.ApiClient",
  "unit": "member"
}
```

</details>
