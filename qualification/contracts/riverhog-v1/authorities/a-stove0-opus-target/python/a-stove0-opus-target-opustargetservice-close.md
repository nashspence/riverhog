# a_stove0_opus_target.OpusTargetService.close

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:a-stove0-opus-target:a-stove0-opus-target-opustargetservice-close:91d040b3ab -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-stove0-opus-target](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-c5abdf8094"></a>
- <a id="s-0ad0dd655a"></a>`distribution`: `a-stove0-opus-target`
- <a id="s-65f1e22232"></a>`module`: `a_stove0_opus_target`
- <a id="s-5df906d59e"></a>`name`: `close`
- <a id="s-d4d6d46b1b"></a>`owner`: `a_stove0_opus_target.OpusTargetService`
- <a id="s-0650c16704"></a>`unit`: `member`

### Declared structure

- <a id="s-00047e2d28"></a>`kind`: `"method"`
- <a id="s-f843eba49f"></a>`signature`: `"\"(self) -> 'None'\""`

## Maintained corroboration

### Related interface records

- [OpusTargetService](a-stove0-opus-target-opustargetservice.md)

## Governing policies

- <a id="pa-591c1d2af6"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:a-stove0-opus-target:a_stove0_opus_target](../../../evidence/sources/authorities.md#src-43f70c74af) — [some-implementations/stove0/targets/opus/target/src/a\_stove0\_opus\_target/\_\_init\_\_.py](../../../../../../some-implementations/stove0/targets/opus/target/src/a_stove0_opus_target/__init__.py)

### Machine authority

- `/external_contract/python/a_stove0_opus_target.OpusTargetService.close`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: d3189f05d0c4b115b223e2f75ab6afcac3b75c2de27ba3e31938d8378b30496a -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'None'\""
  },
  "distribution": "a-stove0-opus-target",
  "module": "a_stove0_opus_target",
  "name": "close",
  "owner": "a_stove0_opus_target.OpusTargetService",
  "unit": "member"
}
```

</details>
