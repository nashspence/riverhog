# riverhog_client.processing.ClaimedCollectionRuntimeRegistry

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-processing-claimedcollect-8131b8493e:0b9200e561 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-a487ad59a7"></a>
- <a id="s-7efa38f19e"></a>`distribution`: `riverhog-client`
- <a id="s-7f814f3f37"></a>`module`: `riverhog_client.processing`
- <a id="s-8443aca518"></a>`name`: `ClaimedCollectionRuntimeRegistry`
- <a id="s-2cf1a4657d"></a>`unit`: `export`

### Declared structure

- <a id="s-af3eef5fe3"></a>`kind`: `"class"`
- <a id="s-6f7bcf8a55"></a>`signature`: `"\"() -> 'None'\""`

## Maintained corroboration

### Related interface records

- [refresh](riverhog-client-processing-claimedcollectionruntimeregistry-refresh.md)
- [discard](riverhog-client-processing-claimedcollectionruntimeregistry-discard.md)
- [bind](riverhog-client-processing-claimedcollectionruntimeregistry-bind.md)

## Governing policies

- <a id="pa-597fce9b5c"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-client:riverhog_client.processing](../../../evidence/sources/authorities.md#src-89057c8bbf) — [packages/riverhog-client/src/riverhog\_client/processing/\_\_init\_\_.py](../../../../../../packages/riverhog-client/src/riverhog_client/processing/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_client.processing.ClaimedCollectionRuntimeRegistry`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: d43fea1e129ac939f534b1ef10964de2b631fdeee132a669596448f334033ac9 -->

```json
{
  "contract": {
    "kind": "class",
    "signature": "\"() -> 'None'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client.processing",
  "name": "ClaimedCollectionRuntimeRegistry",
  "unit": "export"
}
```

</details>
