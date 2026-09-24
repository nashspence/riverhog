# stove0_target_client.DepartureEffectClient

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-client:stove0-target-client-departureeffectclient:0c52c75199 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-84c55059da"></a>
- <a id="s-775ab88ad9"></a>`distribution`: `stove0-target-client`
- <a id="s-193fbfbb2d"></a>`module`: `stove0_target_client`
- <a id="s-bc55436da9"></a>`name`: `DepartureEffectClient`
- <a id="s-8458c9d666"></a>`unit`: `export`

### Declared structure

- <a id="s-1aef745462"></a>`kind`: `"class"`
- <a id="s-e90955cbfa"></a>`signature`: `"\"(base_url: 'str', *, token: 'str \| None' = None, timeout: 'float' = 300.0, allow_insecure_http: 'bool' = False) -> 'None'\""`

## Maintained corroboration

### Related interface records

- [descriptor](stove0-target-client-departureeffectclient-descriptor.md)
- [put_effect](stove0-target-client-departureeffectclient-put-effect.md)

## Governing policies

- <a id="pa-861e30fc64"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-target-client:stove0_target_client](../../../evidence/sources/authorities.md#src-be4c80156f) — [some-implementations/stove0/packages/target-client/src/stove0\_target\_client/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/target-client/src/stove0_target_client/__init__.py)

### Machine authority

- `/external_contract/python/stove0_target_client.DepartureEffectClient`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: f2d14b82fda11e200358e6bf69539a1cb90b24c2547ca6b352d81308ad35251e -->

```json
{
  "contract": {
    "kind": "class",
    "signature": "\"(base_url: 'str', *, token: 'str | None' = None, timeout: 'float' = 300.0, allow_insecure_http: 'bool' = False) -> 'None'\""
  },
  "distribution": "stove0-target-client",
  "module": "stove0_target_client",
  "name": "DepartureEffectClient",
  "unit": "export"
}
```

</details>
