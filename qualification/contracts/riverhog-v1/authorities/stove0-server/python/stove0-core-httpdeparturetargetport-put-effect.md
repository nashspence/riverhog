# stove0_core.HttpDepartureTargetPort.put_effect

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-httpdeparturetargetport-put-effect:8eb5cd5457 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-144094681b"></a>
- <a id="s-f273bc8275"></a>`distribution`: `stove0-server`
- <a id="s-3b2f9c278c"></a>`module`: `stove0_core`
- <a id="s-3dfcedd2de"></a>`name`: `put_effect`
- <a id="s-aaa9396b9d"></a>`owner`: `stove0_core.HttpDepartureTargetPort`
- <a id="s-bf1faa9761"></a>`unit`: `member`

### Declared structure

- <a id="s-07861fc118"></a>`kind`: `"method"`
- <a id="s-1d7b508ceb"></a>`signature`: `"\"(self, registration_id: 'str', intent: 'DepartureEffectIntent') -> 'DepartureEffectReceipt'\""`

## Maintained corroboration

### Related interface records

- [HttpDepartureTargetPort](stove0-core-httpdeparturetargetport.md)

## Governing policies

- <a id="pa-41b5d2471f"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-server:stove0_core](../../../evidence/sources/authorities.md#src-7558b08e7f) — [some-implementations/stove0/application/server/src/stove0\_core/\_\_init\_\_.py](../../../../../../some-implementations/stove0/application/server/src/stove0_core/__init__.py)

### Machine authority

- `/external_contract/python/stove0_core.HttpDepartureTargetPort.put_effect`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 956287321536de31f5a4afd5adc56ea94befadb78a56f475e1abd86784d5c18a -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, registration_id: 'str', intent: 'DepartureEffectIntent') -> 'DepartureEffectReceipt'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "put_effect",
  "owner": "stove0_core.HttpDepartureTargetPort",
  "unit": "member"
}
```

</details>
