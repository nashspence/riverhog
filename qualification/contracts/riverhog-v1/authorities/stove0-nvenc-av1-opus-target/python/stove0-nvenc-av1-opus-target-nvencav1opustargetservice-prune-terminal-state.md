# stove0_nvenc_av1_opus_target.NvencAv1OpusTargetService.prune_terminal_state

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-nvenc-av1-opus-target:stove0-nvenc-av1-opus-target-nvencav1opus-2fc3c61f50:96df16ee36 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-nvenc-av1-opus-target](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-12ac6d4f01"></a>
- <a id="s-b551a79024"></a>`distribution`: `stove0-nvenc-av1-opus-target`
- <a id="s-8c3ef53fab"></a>`module`: `stove0_nvenc_av1_opus_target`
- <a id="s-6c5882e460"></a>`name`: `prune_terminal_state`
- <a id="s-18e05347ab"></a>`owner`: `stove0_nvenc_av1_opus_target.NvencAv1OpusTargetService`
- <a id="s-e4fc154bb8"></a>`unit`: `member`

### Declared structure

- <a id="s-3f33069a97"></a>`kind`: `"method"`
- <a id="s-e48a24c77a"></a>`signature`: `"\"(self, *, now: 'float \| None' = None) -> 'dict[str, int]'\""`

## Maintained corroboration

### Related interface records

- [NvencAv1OpusTargetService](stove0-nvenc-av1-opus-target-nvencav1opustargetservice.md)

## Governing policies

- <a id="pa-3631ee1165"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-nvenc-av1-opus-target:stove0_nvenc_av1_opus_target](../../../evidence/sources/authorities.md#src-b6e7b93ef1) — [reference/stove0/targets/nvenc-av1-opus/target/src/stove0\_nvenc\_av1\_opus\_target/\_\_init\_\_.py](../../../../../../reference/stove0/targets/nvenc-av1-opus/target/src/stove0_nvenc_av1_opus_target/__init__.py)

### Machine authority

- `/external_contract/python/stove0_nvenc_av1_opus_target.NvencAv1OpusTargetService.prune_terminal_state`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: dcb070d1916f319bfbe3018fd7b28c9033986e7476781bbd9e561f3a9d378495 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, *, now: 'float | None' = None) -> 'dict[str, int]'\""
  },
  "distribution": "stove0-nvenc-av1-opus-target",
  "module": "stove0_nvenc_av1_opus_target",
  "name": "prune_terminal_state",
  "owner": "stove0_nvenc_av1_opus_target.NvencAv1OpusTargetService",
  "unit": "member"
}
```

</details>
