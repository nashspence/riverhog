# riverhog_client.transform.DerivedCollectionReceipt.from_mapping

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-transform-derivedcollecti-335d42cc3b:c50a2614ff -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-3b8514d8c2"></a>
- <a id="s-9775e58642"></a>`distribution`: `riverhog-client`
- <a id="s-b56ad3d666"></a>`module`: `riverhog_client.transform`
- <a id="s-ddedaed25a"></a>`name`: `from_mapping`
- <a id="s-961b326212"></a>`owner`: `riverhog_client.transform.DerivedCollectionReceipt`
- <a id="s-fc16fe0d1b"></a>`unit`: `member`

### Declared structure

- <a id="s-0d2ddf51a2"></a>`kind`: `"classmethod"`
- <a id="s-7611e410e8"></a>`signature`: `"\"(cls, value: 'Mapping[str, object]') -> 'DerivedCollectionReceipt'\""`

## Maintained corroboration

### Related interface records

- [DerivedCollectionReceipt](riverhog-client-transform-derivedcollectionreceipt.md)

## Governing policies

- <a id="pa-6b232bafa2"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-client:riverhog_client.transform](../../../evidence/sources/authorities.md#src-7a247bb534) — [packages/riverhog-client/src/riverhog\_client/transform/\_\_init\_\_.py](../../../../../../packages/riverhog-client/src/riverhog_client/transform/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_client.transform.DerivedCollectionReceipt.from_mapping`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 91d054c24471bf47038031fa42b3d6ca41652dca1a7216d959527a2896ef0920 -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, value: 'Mapping[str, object]') -> 'DerivedCollectionReceipt'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client.transform",
  "name": "from_mapping",
  "owner": "riverhog_client.transform.DerivedCollectionReceipt",
  "unit": "member"
}
```

</details>
