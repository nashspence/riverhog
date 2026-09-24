# riverhog_client.processing.DerivedCollectionWriter.replace_api

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-processing-derivedcollect-d2f0fcc5dc:f1cbf5cde7 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-58c674a201"></a>
- <a id="s-592cf9a04b"></a>`distribution`: `riverhog-client`
- <a id="s-d5c1151be5"></a>`module`: `riverhog_client.processing`
- <a id="s-9a37c4b8a9"></a>`name`: `replace_api`
- <a id="s-0421514ef5"></a>`owner`: `riverhog_client.processing.DerivedCollectionWriter`
- <a id="s-e9b74ccbd4"></a>`unit`: `member`

### Declared structure

- <a id="s-0e33a24dcd"></a>`kind`: `"method"`
- <a id="s-726c6d04c4"></a>`signature`: `"\"(self, api: 'Any') -> 'None'\""`

## Maintained corroboration

### Related interface records

- [DerivedCollectionWriter](riverhog-client-processing-derivedcollectionwriter.md)

## Governing policies

- <a id="pa-36505b7a19"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-client:riverhog_client.processing](../../../evidence/sources/authorities.md#src-89057c8bbf) — [packages/riverhog-client/src/riverhog\_client/processing/\_\_init\_\_.py](../../../../../../packages/riverhog-client/src/riverhog_client/processing/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_client.processing.DerivedCollectionWriter.replace_api`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 2c6d2ba22c2520bd5d37ab004fa6c6f5dd8e071420493424109c53ddfb613447 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, api: 'Any') -> 'None'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client.processing",
  "name": "replace_api",
  "owner": "riverhog_client.processing.DerivedCollectionWriter",
  "unit": "member"
}
```

</details>
