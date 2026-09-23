# stove0_core.HttpTargetPort.descriptor

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-httptargetport-descriptor:d7fa7d6a3e -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-3563de4405"></a>
- <a id="s-44735a3867"></a>`distribution`: `stove0-server`
- <a id="s-6b2496ca3f"></a>`module`: `stove0_core`
- <a id="s-a05f69fd52"></a>`name`: `descriptor`
- <a id="s-d34df665df"></a>`owner`: `stove0_core.HttpTargetPort`
- <a id="s-58df94d5f3"></a>`unit`: `member`

### Declared structure

- <a id="s-05356ac03b"></a>`kind`: `"method"`
- <a id="s-bce540a7f6"></a>`signature`: `"\"(self, registration_id: 'str') -> 'TargetDescriptor'\""`

## Maintained corroboration

### Related interface records

- [HttpTargetPort](stove0-core-httptargetport.md)

## Governing policies

- <a id="pa-1cd65ccf34"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-server:stove0_core](../../../evidence/sources/authorities.md#src-7558b08e7f) — [some-implementations/stove0/application/server/src/stove0\_core/\_\_init\_\_.py](../../../../../../some-implementations/stove0/application/server/src/stove0_core/__init__.py)

### Machine authority

- `/external_contract/python/stove0_core.HttpTargetPort.descriptor`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: b1ae5529e92dd267a7d7d8bb200c9edf74c70312ace8e438777955647f7c5324 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, registration_id: 'str') -> 'TargetDescriptor'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "descriptor",
  "owner": "stove0_core.HttpTargetPort",
  "unit": "member"
}
```

</details>
