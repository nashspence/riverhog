# riverhog_client.processing.CollectionTransformRuntime.from_capability

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-processing-collectiontran-8bc9cfe225:c4fdc1ea2b -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-8f887c0088"></a>
- <a id="s-21398bf611"></a>`distribution`: `riverhog-client`
- <a id="s-79001cff82"></a>`module`: `riverhog_client.processing`
- <a id="s-5ab2bad07e"></a>`name`: `from_capability`
- <a id="s-efe6bc1212"></a>`owner`: `riverhog_client.processing.CollectionTransformRuntime`
- <a id="s-7b8d0810a1"></a>`unit`: `member`

### Declared structure

- <a id="s-36f7f4a6ca"></a>`kind`: `"classmethod"`
- <a id="s-dd42e6ccdf"></a>`signature`: `"\"(cls, *, base_url: 'str', capability_token: 'str', spec: 'DerivedCollectionSpec', claim_id: 'str', fence: 'int', work_id: 'str', execution_id: 'str', controller_evidence: 'Mapping[str, object]', allow_insecure_http: 'bool' = False, **kwargs: 'Any') -> 'CollectionTransformRuntime'\""`

## Maintained corroboration

### Related interface records

- [CollectionTransformRuntime](riverhog-client-processing-collectiontransformruntime.md)

## Governing policies

- <a id="pa-4a43dd416c"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-client:riverhog_client.processing](../../../evidence/sources/authorities.md#src-89057c8bbf) — [packages/riverhog-client/src/riverhog\_client/processing/\_\_init\_\_.py](../../../../../../packages/riverhog-client/src/riverhog_client/processing/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_client.processing.CollectionTransformRuntime.from_capability`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 4f70a06db473ff7f83d69593740ca2af2849c7f63373728c70c2c8863e9ecd7f -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, *, base_url: 'str', capability_token: 'str', spec: 'DerivedCollectionSpec', claim_id: 'str', fence: 'int', work_id: 'str', execution_id: 'str', controller_evidence: 'Mapping[str, object]', allow_insecure_http: 'bool' = False, **kwargs: 'Any') -> 'CollectionTransformRuntime'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client.processing",
  "name": "from_capability",
  "owner": "riverhog_client.processing.CollectionTransformRuntime",
  "unit": "member"
}
```

</details>
