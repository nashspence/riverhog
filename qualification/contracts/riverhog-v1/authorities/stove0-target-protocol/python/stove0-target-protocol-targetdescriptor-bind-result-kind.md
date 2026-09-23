# stove0_target_protocol.TargetDescriptor.bind_result_kind

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-protocol:stove0-target-protocol-targetdescriptor-b-8ab22f7cb5:5b17f887f1 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-8c3183ad8e"></a>
- <a id="s-6b29ced033"></a>`distribution`: `stove0-target-protocol`
- <a id="s-a98849f7da"></a>`module`: `stove0_target_protocol`
- <a id="s-361a7d0728"></a>`name`: `bind_result_kind`
- <a id="s-54e5737ffd"></a>`owner`: `stove0_target_protocol.TargetDescriptor`
- <a id="s-121a2330e4"></a>`unit`: `member`

### Declared structure

- <a id="s-ab2aba349a"></a>`kind`: `"method"`
- <a id="s-69edd7e435"></a>`signature`: `"\"(self) -> 'Self'\""`

## Maintained corroboration

### Related interface records

- [TargetDescriptor](stove0-target-protocol-targetdescriptor.md)

## Governing policies

- <a id="pa-99deccbb54"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-target-protocol:stove0_target_protocol](../../../evidence/sources/authorities.md#src-f4f0b22026) — [some-implementations/stove0/packages/target-protocol/src/stove0\_target\_protocol/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/target-protocol/src/stove0_target_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_target_protocol.TargetDescriptor.bind_result_kind`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 41742d55306a703fac226737d3c73837d2e0a813ab6c771ddbe5a25bbb02e8dd -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'Self'\""
  },
  "distribution": "stove0-target-protocol",
  "module": "stove0_target_protocol",
  "name": "bind_result_kind",
  "owner": "stove0_target_protocol.TargetDescriptor",
  "unit": "member"
}
```

</details>
