# stove0_target_client.DepartureEffectClient.descriptor

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-client:stove0-target-client-departureeffectclien-d50ab89e75:474b9fd387 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-de2c44428e"></a>
- <a id="s-202a09ff46"></a>`distribution`: `stove0-target-client`
- <a id="s-a21c4b3fc9"></a>`module`: `stove0_target_client`
- <a id="s-3caa2339dd"></a>`name`: `descriptor`
- <a id="s-77b43770b7"></a>`owner`: `stove0_target_client.DepartureEffectClient`
- <a id="s-367c2f3e69"></a>`unit`: `member`

### Declared structure

- <a id="s-9cf112d0e0"></a>`kind`: `"method"`
- <a id="s-7003ba8fef"></a>`signature`: `"\"(self) -> 'DepartureEffectTargetDescriptor'\""`

## Maintained corroboration

### Related interface records

- [DepartureEffectClient](stove0-target-client-departureeffectclient.md)

## Governing policies

- <a id="pa-d9799e42a9"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-target-client:stove0_target_client](../../../evidence/sources/authorities.md#src-be4c80156f) — [some-implementations/stove0/packages/target-client/src/stove0\_target\_client/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/target-client/src/stove0_target_client/__init__.py)

### Machine authority

- `/external_contract/python/stove0_target_client.DepartureEffectClient.descriptor`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: d479112333b97556e27f3f90e1ec4f9a80997b09a4e494bc3a353d5a0c28f24b -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'DepartureEffectTargetDescriptor'\""
  },
  "distribution": "stove0-target-client",
  "module": "stove0_target_client",
  "name": "descriptor",
  "owner": "stove0_target_client.DepartureEffectClient",
  "unit": "member"
}
```

</details>
