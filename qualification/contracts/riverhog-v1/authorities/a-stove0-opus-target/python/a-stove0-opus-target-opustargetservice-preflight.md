# a_stove0_opus_target.OpusTargetService.preflight

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:a-stove0-opus-target:a-stove0-opus-target-opustargetservice-preflight:4e8d557bc7 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-stove0-opus-target](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-beeb115bcb"></a>
- <a id="s-8e1b49df2a"></a>`distribution`: `a-stove0-opus-target`
- <a id="s-b7afaf3383"></a>`module`: `a_stove0_opus_target`
- <a id="s-63cb7d40be"></a>`name`: `preflight`
- <a id="s-ce5d984dc7"></a>`owner`: `a_stove0_opus_target.OpusTargetService`
- <a id="s-9227d89e57"></a>`unit`: `member`

### Declared structure

- <a id="s-5d686edf05"></a>`kind`: `"method"`
- <a id="s-f7e7ab88b9"></a>`signature`: `"\"(self, request: 'TargetPreflightRequest') -> 'TargetPreflightResponse'\""`

## Maintained corroboration

### Related interface records

- [OpusTargetService](a-stove0-opus-target-opustargetservice.md)

## Governing policies

- <a id="pa-2f37b74613"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:a-stove0-opus-target:a_stove0_opus_target](../../../evidence/sources/authorities.md#src-43f70c74af) — [some-implementations/stove0/targets/opus/target/src/a\_stove0\_opus\_target/\_\_init\_\_.py](../../../../../../some-implementations/stove0/targets/opus/target/src/a_stove0_opus_target/__init__.py)

### Machine authority

- `/external_contract/python/a_stove0_opus_target.OpusTargetService.preflight`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: cd2c6bda4eb7fb0ea04b18ba578ca09d8188fddb8379a213f4ce75ed51582560 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, request: 'TargetPreflightRequest') -> 'TargetPreflightResponse'\""
  },
  "distribution": "a-stove0-opus-target",
  "module": "a_stove0_opus_target",
  "name": "preflight",
  "owner": "a_stove0_opus_target.OpusTargetService",
  "unit": "member"
}
```

</details>
