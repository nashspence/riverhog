# a_stove0_nvenc_av1_opus_target.NvencAv1OpusTargetService.prune_terminal_state

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:a-stove0-nvenc-av1-opus-target:a-stove0-nvenc-av1-opus-target-nvencav1op-4cb11a4787:9835f42729 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-stove0-nvenc-av1-opus-target](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-04800b1874"></a>
- <a id="s-eed892f95a"></a>`distribution`: `a-stove0-nvenc-av1-opus-target`
- <a id="s-00445702aa"></a>`module`: `a_stove0_nvenc_av1_opus_target`
- <a id="s-8ec8808f7a"></a>`name`: `prune_terminal_state`
- <a id="s-a3ba1db653"></a>`owner`: `a_stove0_nvenc_av1_opus_target.NvencAv1OpusTargetService`
- <a id="s-54b4d54e7f"></a>`unit`: `member`

### Declared structure

- <a id="s-9b0c05ec7a"></a>`kind`: `"method"`
- <a id="s-06c0db0d8d"></a>`signature`: `"\"(self, *, now: 'float \| None' = None) -> 'dict[str, int]'\""`

## Maintained corroboration

### Related interface records

- [NvencAv1OpusTargetService](a-stove0-nvenc-av1-opus-target-nvencav1opustargetservice.md)

## Governing policies

- <a id="pa-42e2ff9250"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:a-stove0-nvenc-av1-opus-target:a_stove0_nvenc_av1_opus_target](../../../evidence/sources/authorities.md#src-a28a72aec7) — [some-implementations/stove0/targets/nvenc-av1-opus/target/src/a\_stove0\_nvenc\_av1\_opus\_target/\_\_init\_\_.py](../../../../../../some-implementations/stove0/targets/nvenc-av1-opus/target/src/a_stove0_nvenc_av1_opus_target/__init__.py)

### Machine authority

- `/external_contract/python/a_stove0_nvenc_av1_opus_target.NvencAv1OpusTargetService.prune_terminal_state`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: e77a13a1385a90079630b56bc74cec56f9b6d9ecccc5c8f772fa76d6b25a6839 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, *, now: 'float | None' = None) -> 'dict[str, int]'\""
  },
  "distribution": "a-stove0-nvenc-av1-opus-target",
  "module": "a_stove0_nvenc_av1_opus_target",
  "name": "prune_terminal_state",
  "owner": "a_stove0_nvenc_av1_opus_target.NvencAv1OpusTargetService",
  "unit": "member"
}
```

</details>
