# riverhog_client.ApiClient.advance_retrieval_plan

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-apiclient-advance-retrieval-plan:02998f65b7 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-6adc0150db"></a>
- <a id="s-b5746936db"></a>`distribution`: `riverhog-client`
- <a id="s-77d08ba66b"></a>`module`: `riverhog_client`
- <a id="s-b256f86f5a"></a>`name`: `advance_retrieval_plan`
- <a id="s-181439ea56"></a>`owner`: `riverhog_client.ApiClient`
- <a id="s-f76f5352f6"></a>`unit`: `member`

### Declared structure

- <a id="s-a8202f8229"></a>`kind`: `"method"`
- <a id="s-540095c04c"></a>`signature`: `"\"(self, plan_id: 'str') -> 'dict[str, Any]'\""`

## Maintained corroboration

### Related interface records

- [piggity local repair](../../piggity/cli/piggity-local-repair.md)
- [piggity local sync](../../piggity/cli/piggity-local-sync.md)
- [POST /v1/retrieval-plans/{plan_id}/advance](../../riverhog/http-operations/post-v1-retrieval-plans-plan-id-advance.md)
- [ApiClient](riverhog-client-apiclient.md)

## Governing policies

- <a id="pa-856d2871c2"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-client:riverhog_client](../../../evidence/sources/authorities.md#src-c149020c71) — [packages/riverhog-client/src/riverhog\_client/\_\_init\_\_.py](../../../../../../packages/riverhog-client/src/riverhog_client/__init__.py)
- **Client method:** [packages/riverhog-client/src/riverhog\_client/client.py::ApiClient.advance\_retrieval\_plan](../../../../../../packages/riverhog-client/src/riverhog_client/client.py#L834)

### Machine authority

- `/external_contract/python/riverhog_client.ApiClient.advance_retrieval_plan`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 5a9b5ac8f0c39e04b1f1588dd809019d9157fe9bf7781a00bdd2ad70b9d9a43d -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, plan_id: 'str') -> 'dict[str, Any]'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client",
  "name": "advance_retrieval_plan",
  "owner": "riverhog_client.ApiClient",
  "unit": "member"
}
```

</details>
