# stove0_core.DepartureEffectService.get_effect

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-departureeffectservice-get-effect:281b3b99e7 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-c6f6664c21"></a>
- <a id="s-c129a2d297"></a>`distribution`: `stove0-server`
- <a id="s-a1d76e9984"></a>`module`: `stove0_core`
- <a id="s-a9a3b6fbd3"></a>`name`: `get_effect`
- <a id="s-0d07e49501"></a>`owner`: `stove0_core.DepartureEffectService`
- <a id="s-81d189b5ce"></a>`unit`: `member`

### Declared structure

- <a id="s-c1ee883acc"></a>`kind`: `"method"`
- <a id="s-9d473996a8"></a>`signature`: `"\"(self, departure_id: 'str') -> 'DepartureEffectView'\""`

## Maintained corroboration

### Related interface records

- [DepartureEffectService](stove0-core-departureeffectservice.md)

## Governing policies

- <a id="pa-95662ef913"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-server:stove0_core](../../../evidence/sources/authorities.md#src-7558b08e7f) — [some-implementations/stove0/application/server/src/stove0\_core/\_\_init\_\_.py](../../../../../../some-implementations/stove0/application/server/src/stove0_core/__init__.py)

### Machine authority

- `/external_contract/python/stove0_core.DepartureEffectService.get_effect`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 7efc506868e6747dbef47b621d4962e9352d7ada0a761fabb5c6d7fdad0288c7 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, departure_id: 'str') -> 'DepartureEffectView'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "get_effect",
  "owner": "stove0_core.DepartureEffectService",
  "unit": "member"
}
```

</details>
