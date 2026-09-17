# riverhog_protocol.PortableCollectionInventoryPage.validate_page

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-portablecollectioninven-540a77e796:922be6bc49 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-75d3a199d7"></a>
- <a id="s-70184ab45e"></a>`distribution`: `riverhog-protocol`
- <a id="s-b7c0af9e93"></a>`module`: `riverhog_protocol`
- <a id="s-3002a1af73"></a>`name`: `validate_page`
- <a id="s-91bf33d5bd"></a>`owner`: `riverhog_protocol.PortableCollectionInventoryPage`
- <a id="s-15ee36fead"></a>`unit`: `member`

### Declared structure

- <a id="s-80b51661da"></a>`kind`: `"method"`
- <a id="s-ad8b39e7be"></a>`signature`: `"\"(self) -> 'PortableCollectionInventoryPage'\""`

## Maintained corroboration

### Related interface records

- [PortableCollectionInventoryPage](riverhog-protocol-portablecollectioninventorypage.md)

## Governing policies

- <a id="pa-2a97a36159"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources/authorities.md#src-19e35f15d9) — [packages/riverhog-protocol/src/riverhog\_protocol/\_\_init\_\_.py](../../../../../../packages/riverhog-protocol/src/riverhog_protocol/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_protocol.PortableCollectionInventoryPage.validate_page`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 5f9688b1264a821fc4ac3e4dc6966576a30e5c7dbe7e6f22bc44e221192e875d -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'PortableCollectionInventoryPage'\""
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "validate_page",
  "owner": "riverhog_protocol.PortableCollectionInventoryPage",
  "unit": "member"
}
```

</details>
