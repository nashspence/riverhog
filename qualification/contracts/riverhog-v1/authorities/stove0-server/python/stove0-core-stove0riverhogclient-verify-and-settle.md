# stove0_core.Stove0RiverhogClient.verify_and_settle

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-stove0riverhogclient-verify-and-settle:18850ae6fc -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-da926cc496"></a>
- <a id="s-067fad56ef"></a>`distribution`: `stove0-server`
- <a id="s-0e44ad5334"></a>`module`: `stove0_core`
- <a id="s-9c82aa6ae0"></a>`name`: `verify_and_settle`
- <a id="s-b715e10f28"></a>`owner`: `stove0_core.Stove0RiverhogClient`
- <a id="s-21219d1892"></a>`unit`: `member`

### Declared structure

- <a id="s-8147c0bd26"></a>`kind`: `"method"`
- <a id="s-fcbd204074"></a>`signature`: `"\"(self, record: 'WorkRecord', parent_outcome: 'ParentOutcomeBinding \| None' = None) -> 'tuple[OutputCollectionRef, TargetSettlementAuthority \| None]'\""`

## Maintained corroboration

### Related interface records

- [Stove0RiverhogClient](stove0-core-stove0riverhogclient.md)

## Governing policies

- <a id="pa-f1284e490b"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-server:stove0_core](../../../evidence/sources/authorities.md#src-7558b08e7f) — [reference/stove0/application/server/src/stove0\_core/\_\_init\_\_.py](../../../../../../reference/stove0/application/server/src/stove0_core/__init__.py)

### Machine authority

- `/external_contract/python/stove0_core.Stove0RiverhogClient.verify_and_settle`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 4b03048306fba726a2c9707b55a0aa3a67ebb377d8f6d85b6bfe23a4285b1683 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, record: 'WorkRecord', parent_outcome: 'ParentOutcomeBinding | None' = None) -> 'tuple[OutputCollectionRef, TargetSettlementAuthority | None]'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "verify_and_settle",
  "owner": "stove0_core.Stove0RiverhogClient",
  "unit": "member"
}
```

</details>
