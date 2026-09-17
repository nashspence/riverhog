# riverhog_client.transform.DerivedCollectionWriter

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-transform-derivedcollectionwriter:4331882d8f -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-0227d44d56"></a>
- <a id="s-67d42169ef"></a>`distribution`: `riverhog-client`
- <a id="s-d3d8cb5414"></a>`module`: `riverhog_client.transform`
- <a id="s-035fa63ff0"></a>`name`: `DerivedCollectionWriter`
- <a id="s-0604f50062"></a>`unit`: `export`

### Declared structure

- <a id="s-4259e9def4"></a>`kind`: `"class"`
- <a id="s-7d52663eeb"></a>`signature`: `"\"(api: 'Any', *, spec: 'DerivedCollectionSpec', claim_id: 'str', fence: 'int', work_id: 'str', execution_id: 'str', controller_evidence: 'Mapping[str, object]', producer_app: 'str', producer_version: 'str' = 'development') -> 'None'\""`

## Maintained corroboration

### Related interface records

- [replace_api](riverhog-client-transform-derivedcollectionwriter-replace-api.md)
- [publish](riverhog-client-transform-derivedcollectionwriter-publish.md)

## Governing policies

- <a id="pa-38413b040f"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-client:riverhog_client.transform](../../../evidence/sources.md#src-7a247bb534) — [packages/riverhog-client/src/riverhog\_client/transform/\_\_init\_\_.py](../../../../../../packages/riverhog-client/src/riverhog_client/transform/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_client.transform.DerivedCollectionWriter`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: e30a3c59ba40e83abc6bff1c50471c5e381ee2bbeba32c84e1cd47c1f7f27a79 -->

```json
{
  "contract": {
    "kind": "class",
    "signature": "\"(api: 'Any', *, spec: 'DerivedCollectionSpec', claim_id: 'str', fence: 'int', work_id: 'str', execution_id: 'str', controller_evidence: 'Mapping[str, object]', producer_app: 'str', producer_version: 'str' = 'development') -> 'None'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client.transform",
  "name": "DerivedCollectionWriter",
  "unit": "export"
}
```

</details>
