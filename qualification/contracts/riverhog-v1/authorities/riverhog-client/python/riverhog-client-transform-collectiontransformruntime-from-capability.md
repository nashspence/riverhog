# riverhog_client.transform.CollectionTransformRuntime.from_capability

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-transform-collectiontrans-072dfc2841:dcd77b4fd4 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-2ba50ff0ba"></a>
- <a id="s-629f7bd5c5"></a>`distribution`: `riverhog-client`
- <a id="s-e372050583"></a>`module`: `riverhog_client.transform`
- <a id="s-d617fa70a8"></a>`name`: `from_capability`
- <a id="s-c2f6553901"></a>`owner`: `riverhog_client.transform.CollectionTransformRuntime`
- <a id="s-22ce9ef691"></a>`unit`: `member`

### Declared structure

- <a id="s-db02ac4b27"></a>`kind`: `"classmethod"`
- <a id="s-d15b92c6d6"></a>`signature`: `"\"(cls, *, base_url: 'str', capability_token: 'str', spec: 'DerivedCollectionSpec', claim_id: 'str', fence: 'int', work_id: 'str', execution_id: 'str', controller_evidence: 'Mapping[str, object]', allow_insecure_http: 'bool' = False, **kwargs: 'Any') -> 'CollectionTransformRuntime'\""`

## Maintained corroboration

### Related interface records

- [riverhog_client.transform.CollectionTransformRuntime](riverhog-client-transform-collectiontransformruntime.md)

## Governing policies

- <a id="pa-406c6084aa"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-client:riverhog_client.transform](../../../evidence/sources.md#src-7a247bb534) — `packages/riverhog-client/src/riverhog_client/transform/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_client.transform.CollectionTransformRuntime.from_capability`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: e360b03313302c3b45187101e7f410700ab5aa2d84ae8d32f986e79aff575234 -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, *, base_url: 'str', capability_token: 'str', spec: 'DerivedCollectionSpec', claim_id: 'str', fence: 'int', work_id: 'str', execution_id: 'str', controller_evidence: 'Mapping[str, object]', allow_insecure_http: 'bool' = False, **kwargs: 'Any') -> 'CollectionTransformRuntime'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client.transform",
  "name": "from_capability",
  "owner": "riverhog_client.transform.CollectionTransformRuntime",
  "unit": "member"
}
```
