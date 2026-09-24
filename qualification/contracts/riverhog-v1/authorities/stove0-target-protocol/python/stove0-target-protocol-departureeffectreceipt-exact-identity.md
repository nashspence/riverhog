# stove0_target_protocol.DepartureEffectReceipt.exact_identity

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-protocol:stove0-target-protocol-departureeffectrec-1b03e967de:7cfb3c8f65 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-c9fa049534"></a>
- <a id="s-a3a75259ea"></a>`distribution`: `stove0-target-protocol`
- <a id="s-5781f72bb8"></a>`module`: `stove0_target_protocol`
- <a id="s-355df07493"></a>`name`: `exact_identity`
- <a id="s-1f1300eecb"></a>`owner`: `stove0_target_protocol.DepartureEffectReceipt`
- <a id="s-242052a14a"></a>`unit`: `member`

### Declared structure

- <a id="s-a341e5b9c6"></a>`kind`: `"method"`
- <a id="s-f6a8d4e987"></a>`signature`: `"\"(self) -> 'Self'\""`

## Maintained corroboration

### Related interface records

- [DepartureEffectReceipt](stove0-target-protocol-departureeffectreceipt.md)

## Governing policies

- <a id="pa-c69a1a3c20"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-target-protocol:stove0_target_protocol](../../../evidence/sources/authorities.md#src-f4f0b22026) — [some-implementations/stove0/packages/target-protocol/src/stove0\_target\_protocol/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/target-protocol/src/stove0_target_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_target_protocol.DepartureEffectReceipt.exact_identity`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: c3f5283554590dd5aa2c922b76ef3b302855d89dbad58545e3cf296a1947d74e -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'Self'\""
  },
  "distribution": "stove0-target-protocol",
  "module": "stove0_target_protocol",
  "name": "exact_identity",
  "owner": "stove0_target_protocol.DepartureEffectReceipt",
  "unit": "member"
}
```

</details>
