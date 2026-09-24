# riverhog_client.processing.DerivedCollectionReceipt.from_mapping

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-processing-derivedcollect-19c24afaa9:244357db35 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-d37a9c2944"></a>
- <a id="s-7d02634324"></a>`distribution`: `riverhog-client`
- <a id="s-226e0b34b7"></a>`module`: `riverhog_client.processing`
- <a id="s-dbe0a899ff"></a>`name`: `from_mapping`
- <a id="s-ac75681a80"></a>`owner`: `riverhog_client.processing.DerivedCollectionReceipt`
- <a id="s-ec50542af8"></a>`unit`: `member`

### Declared structure

- <a id="s-aa0cc81abb"></a>`kind`: `"classmethod"`
- <a id="s-90e9d27440"></a>`signature`: `"\"(cls, value: 'Mapping[str, object]') -> 'DerivedCollectionReceipt'\""`

## Maintained corroboration

### Related interface records

- [DerivedCollectionReceipt](riverhog-client-processing-derivedcollectionreceipt.md)

## Governing policies

- <a id="pa-329a45474e"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-client:riverhog_client.processing](../../../evidence/sources/authorities.md#src-89057c8bbf) — [packages/riverhog-client/src/riverhog\_client/processing/\_\_init\_\_.py](../../../../../../packages/riverhog-client/src/riverhog_client/processing/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_client.processing.DerivedCollectionReceipt.from_mapping`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: b72e49d16cecfbc42e6726381d92d8339e63029159795c53f69d6f66eb21733d -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, value: 'Mapping[str, object]') -> 'DerivedCollectionReceipt'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client.processing",
  "name": "from_mapping",
  "owner": "riverhog_client.processing.DerivedCollectionReceipt",
  "unit": "member"
}
```

</details>
