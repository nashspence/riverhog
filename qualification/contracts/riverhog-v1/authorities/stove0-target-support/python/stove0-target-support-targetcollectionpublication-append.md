# stove0_target_support.TargetCollectionPublication.append

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-support:stove0-target-support-targetcollectionpub-83487376b4:6449b86a98 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-1f6af6a011"></a>
- <a id="s-23990c0b6e"></a>`distribution`: `stove0-target-support`
- <a id="s-e65090a730"></a>`module`: `stove0_target_support`
- <a id="s-851917c047"></a>`name`: `append`
- <a id="s-a715078922"></a>`owner`: `stove0_target_support.TargetCollectionPublication`
- <a id="s-2798291a46"></a>`unit`: `member`

### Declared structure

- <a id="s-f7707995ba"></a>`kind`: `"method"`
- <a id="s-17014e829a"></a>`signature`: `"\"(self, source: 'ProducerInput', artifact: 'OutputArtifact', *, derived_from: 'Iterable[str]') -> 'tuple[ProducerArtifactCustody, ...]'\""`

## Maintained corroboration

### Related interface records

- [TargetCollectionPublication](stove0-target-support-targetcollectionpublication.md)

## Governing policies

- <a id="pa-9046f9a7c3"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-target-support:stove0_target_support](../../../evidence/sources/authorities.md#src-3c01163237) — [reference/stove0/packages/target-support/src/stove0\_target\_support/\_\_init\_\_.py](../../../../../../reference/stove0/packages/target-support/src/stove0_target_support/__init__.py)

### Machine authority

- `/external_contract/python/stove0_target_support.TargetCollectionPublication.append`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 705dcaf41f99a715ad5603c0e0703ba848c4ec98a3c03d97d978ee0eb4ef143d -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, source: 'ProducerInput', artifact: 'OutputArtifact', *, derived_from: 'Iterable[str]') -> 'tuple[ProducerArtifactCustody, ...]'\""
  },
  "distribution": "stove0-target-support",
  "module": "stove0_target_support",
  "name": "append",
  "owner": "stove0_target_support.TargetCollectionPublication",
  "unit": "member"
}
```

</details>
