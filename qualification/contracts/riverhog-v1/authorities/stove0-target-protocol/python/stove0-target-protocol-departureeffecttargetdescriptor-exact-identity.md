# stove0_target_protocol.DepartureEffectTargetDescriptor.exact_identity

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-protocol:stove0-target-protocol-departureeffecttar-47e55e1fca:423c5d8392 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-2f43aca544"></a>
- <a id="s-445c5efc9e"></a>`distribution`: `stove0-target-protocol`
- <a id="s-cc440dd628"></a>`module`: `stove0_target_protocol`
- <a id="s-192699b703"></a>`name`: `exact_identity`
- <a id="s-39c58f1b9e"></a>`owner`: `stove0_target_protocol.DepartureEffectTargetDescriptor`
- <a id="s-e58271c8e1"></a>`unit`: `member`

### Declared structure

- <a id="s-0d370e0d7b"></a>`kind`: `"method"`
- <a id="s-d7ae317ddd"></a>`signature`: `"\"(self) -> 'Self'\""`

## Maintained corroboration

### Related interface records

- [DepartureEffectTargetDescriptor](stove0-target-protocol-departureeffecttargetdescriptor.md)

## Governing policies

- <a id="pa-bad5cae667"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-target-protocol:stove0_target_protocol](../../../evidence/sources/authorities.md#src-f4f0b22026) — [some-implementations/stove0/packages/target-protocol/src/stove0\_target\_protocol/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/target-protocol/src/stove0_target_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_target_protocol.DepartureEffectTargetDescriptor.exact_identity`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: d2d49f082016f3379c96e2d423eacaefe2834be66cc3ff0a7ae25c84b33226c1 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'Self'\""
  },
  "distribution": "stove0-target-protocol",
  "module": "stove0_target_protocol",
  "name": "exact_identity",
  "owner": "stove0_target_protocol.DepartureEffectTargetDescriptor",
  "unit": "member"
}
```

</details>
