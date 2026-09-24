# riverhog_client.processing.CollectionTransformRuntime.__exit__

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-processing-collectiontran-37fe3539a4:411dea535a -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-b146be3b7c"></a>
- <a id="s-1f4b757897"></a>`distribution`: `riverhog-client`
- <a id="s-fdee91d480"></a>`module`: `riverhog_client.processing`
- <a id="s-5ab563d2f2"></a>`name`: `__exit__`
- <a id="s-88e6f80fd5"></a>`owner`: `riverhog_client.processing.CollectionTransformRuntime`
- <a id="s-4130bc43e1"></a>`unit`: `member`

### Declared structure

- <a id="s-b4b55c6c03"></a>`kind`: `"method"`
- <a id="s-b36280fec7"></a>`signature`: `"\"(self, _exc_type: 'object', _exc: 'object', _tb: 'object') -> 'None'\""`

## Maintained corroboration

### Related interface records

- [CollectionTransformRuntime](riverhog-client-processing-collectiontransformruntime.md)

## Governing policies

- <a id="pa-d49b422196"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-client:riverhog_client.processing](../../../evidence/sources/authorities.md#src-89057c8bbf) — [packages/riverhog-client/src/riverhog\_client/processing/\_\_init\_\_.py](../../../../../../packages/riverhog-client/src/riverhog_client/processing/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_client.processing.CollectionTransformRuntime.__exit__`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 3c122ba0c669f9ce048cbc8c8f4b7604c047ee3ed6a92f7accc172270303c8c6 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, _exc_type: 'object', _exc: 'object', _tb: 'object') -> 'None'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client.processing",
  "name": "__exit__",
  "owner": "riverhog_client.processing.CollectionTransformRuntime",
  "unit": "member"
}
```

</details>
