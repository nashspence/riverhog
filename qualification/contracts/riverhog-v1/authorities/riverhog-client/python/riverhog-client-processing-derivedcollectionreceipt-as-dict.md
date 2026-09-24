# riverhog_client.processing.DerivedCollectionReceipt.as_dict

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-processing-derivedcollect-be986fb10e:3ba9586165 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-57fdb50bfd"></a>
- <a id="s-75bf0f6f76"></a>`distribution`: `riverhog-client`
- <a id="s-48255969da"></a>`module`: `riverhog_client.processing`
- <a id="s-82a7269fe7"></a>`name`: `as_dict`
- <a id="s-56fdbcf6a6"></a>`owner`: `riverhog_client.processing.DerivedCollectionReceipt`
- <a id="s-97f18d8d12"></a>`unit`: `member`

### Declared structure

- <a id="s-631bdacff5"></a>`kind`: `"method"`
- <a id="s-35e406ce6d"></a>`signature`: `"\"(self) -> 'dict[str, object]'\""`

## Maintained corroboration

### Related interface records

- [DerivedCollectionReceipt](riverhog-client-processing-derivedcollectionreceipt.md)

## Governing policies

- <a id="pa-cecd4e7836"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-client:riverhog_client.processing](../../../evidence/sources/authorities.md#src-89057c8bbf) — [packages/riverhog-client/src/riverhog\_client/processing/\_\_init\_\_.py](../../../../../../packages/riverhog-client/src/riverhog_client/processing/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_client.processing.DerivedCollectionReceipt.as_dict`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 9b2d835f7e386d20e47d53d93ce7c1a4e38131b8eafce03f150877250ff66e99 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'dict[str, object]'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client.processing",
  "name": "as_dict",
  "owner": "riverhog_client.processing.DerivedCollectionReceipt",
  "unit": "member"
}
```

</details>
