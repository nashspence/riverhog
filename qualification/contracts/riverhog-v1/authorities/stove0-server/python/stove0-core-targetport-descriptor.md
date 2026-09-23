# stove0_core.TargetPort.descriptor

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-targetport-descriptor:8848d43549 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-975f5f9c59"></a>
- <a id="s-ce4d238827"></a>`distribution`: `stove0-server`
- <a id="s-a08f04280d"></a>`module`: `stove0_core`
- <a id="s-8a94b603aa"></a>`name`: `descriptor`
- <a id="s-1a0970775a"></a>`owner`: `stove0_core.TargetPort`
- <a id="s-e8f75567bf"></a>`unit`: `member`

### Declared structure

- <a id="s-cb31fb4f7d"></a>`kind`: `"method"`
- <a id="s-d371cd32cc"></a>`signature`: `"\"(self, registration_id: 'str') -> 'TargetDescriptor'\""`

## Maintained corroboration

### Related interface records

- [TargetPort](stove0-core-targetport.md)

## Governing policies

- <a id="pa-463c67ba19"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-server:stove0_core](../../../evidence/sources/authorities.md#src-7558b08e7f) — [some-implementations/stove0/application/server/src/stove0\_core/\_\_init\_\_.py](../../../../../../some-implementations/stove0/application/server/src/stove0_core/__init__.py)

### Machine authority

- `/external_contract/python/stove0_core.TargetPort.descriptor`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: fc7a5bf9bd0191e32d0ecec12bef8e737caef0d19b7717db64bd22e30490f205 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, registration_id: 'str') -> 'TargetDescriptor'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "descriptor",
  "owner": "stove0_core.TargetPort",
  "unit": "member"
}
```

</details>
