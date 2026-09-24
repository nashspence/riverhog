# stove0_target_client.DepartureEffectClient.put_effect

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-client:stove0-target-client-departureeffectclien-e1bbcfbb92:a459c36ace -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-8382bd5ca5"></a>
- <a id="s-e0a233135e"></a>`distribution`: `stove0-target-client`
- <a id="s-250cb35103"></a>`module`: `stove0_target_client`
- <a id="s-188ae89684"></a>`name`: `put_effect`
- <a id="s-2423300ef3"></a>`owner`: `stove0_target_client.DepartureEffectClient`
- <a id="s-84f956459c"></a>`unit`: `member`

### Declared structure

- <a id="s-ceb1fca361"></a>`kind`: `"method"`
- <a id="s-76f5ed9790"></a>`signature`: `"\"(self, intent: 'DepartureEffectIntent') -> 'DepartureEffectReceipt'\""`

## Maintained corroboration

### Related interface records

- [DepartureEffectClient](stove0-target-client-departureeffectclient.md)

## Governing policies

- <a id="pa-e6700252cb"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-target-client:stove0_target_client](../../../evidence/sources/authorities.md#src-be4c80156f) — [some-implementations/stove0/packages/target-client/src/stove0\_target\_client/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/target-client/src/stove0_target_client/__init__.py)

### Machine authority

- `/external_contract/python/stove0_target_client.DepartureEffectClient.put_effect`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: c3710f915d56df1695ba5dc2eac4a64cd1172a53c155fc45282287cc527e7c59 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, intent: 'DepartureEffectIntent') -> 'DepartureEffectReceipt'\""
  },
  "distribution": "stove0-target-client",
  "module": "stove0_target_client",
  "name": "put_effect",
  "owner": "stove0_target_client.DepartureEffectClient",
  "unit": "member"
}
```

</details>
