# riverhog_client.transform.DerivedCollectionWriter.replace_api

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-transform-derivedcollecti-2a7a37675f:ee6ad51ba4 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-dde0313542"></a>
- <a id="s-c6e22f6ef9"></a>`distribution`: `riverhog-client`
- <a id="s-a4bd3004bc"></a>`module`: `riverhog_client.transform`
- <a id="s-3a6e81aa2d"></a>`name`: `replace_api`
- <a id="s-44b082d0e3"></a>`owner`: `riverhog_client.transform.DerivedCollectionWriter`
- <a id="s-87c4e47543"></a>`unit`: `member`

### Declared structure

- <a id="s-2e1f167242"></a>`kind`: `"method"`
- <a id="s-f6a036ac09"></a>`signature`: `"\"(self, api: 'Any') -> 'None'\""`

## Maintained corroboration

### Related interface records

- [DerivedCollectionWriter](riverhog-client-transform-derivedcollectionwriter.md)

## Governing policies

- <a id="pa-6be7deddf2"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-client:riverhog_client.transform](../../../evidence/sources/authorities.md#src-7a247bb534) — [packages/riverhog-client/src/riverhog\_client/transform/\_\_init\_\_.py](../../../../../../packages/riverhog-client/src/riverhog_client/transform/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_client.transform.DerivedCollectionWriter.replace_api`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: e80b98838d866b1b864bf53b1e92a7c57f2d9c2ab65fa442ee5a87c3b6d0ba52 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, api: 'Any') -> 'None'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client.transform",
  "name": "replace_api",
  "owner": "riverhog_client.transform.DerivedCollectionWriter",
  "unit": "member"
}
```

</details>
