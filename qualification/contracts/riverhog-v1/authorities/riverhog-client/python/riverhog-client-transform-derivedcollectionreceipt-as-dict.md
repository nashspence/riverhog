# riverhog_client.transform.DerivedCollectionReceipt.as_dict

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-transform-derivedcollecti-d4851cdc2c:6b5d27ac9e -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-9501f83920"></a>
- <a id="s-dad5df8c4a"></a>`distribution`: `riverhog-client`
- <a id="s-cca8a971d0"></a>`module`: `riverhog_client.transform`
- <a id="s-e663a1c61e"></a>`name`: `as_dict`
- <a id="s-7784919512"></a>`owner`: `riverhog_client.transform.DerivedCollectionReceipt`
- <a id="s-22519575ea"></a>`unit`: `member`

### Declared structure

- <a id="s-29b80216f8"></a>`kind`: `"method"`
- <a id="s-512843d800"></a>`signature`: `"\"(self) -> 'dict[str, object]'\""`

## Maintained corroboration

### Related interface records

- [DerivedCollectionReceipt](riverhog-client-transform-derivedcollectionreceipt.md)

## Governing policies

- <a id="pa-347c5dda7e"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-client:riverhog_client.transform](../../../evidence/sources.md#src-7a247bb534) — `packages/riverhog-client/src/riverhog_client/transform/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_client.transform.DerivedCollectionReceipt.as_dict`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: fc643f631603296ebf963b7b51a309ad4e7103904cb95781bbd05927dbf910ba -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'dict[str, object]'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client.transform",
  "name": "as_dict",
  "owner": "riverhog_client.transform.DerivedCollectionReceipt",
  "unit": "member"
}
```

</details>
