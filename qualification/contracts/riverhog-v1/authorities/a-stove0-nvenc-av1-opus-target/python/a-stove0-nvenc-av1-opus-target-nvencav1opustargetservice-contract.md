# a_stove0_nvenc_av1_opus_target.NvencAv1OpusTargetService.contract

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:a-stove0-nvenc-av1-opus-target:a-stove0-nvenc-av1-opus-target-nvencav1op-df472fa6c2:7c4256f907 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-stove0-nvenc-av1-opus-target](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-daccf7c4d5"></a>
- <a id="s-a0494e895b"></a>`distribution`: `a-stove0-nvenc-av1-opus-target`
- <a id="s-00f8b12831"></a>`module`: `a_stove0_nvenc_av1_opus_target`
- <a id="s-ad1a13b3b7"></a>`name`: `contract`
- <a id="s-c0a05bb3bc"></a>`owner`: `a_stove0_nvenc_av1_opus_target.NvencAv1OpusTargetService`
- <a id="s-5de42d9c8c"></a>`unit`: `member`

### Declared structure

- <a id="s-7d8db0684d"></a>`kind`: `"method"`
- <a id="s-70306db8d7"></a>`signature`: `"\"(self) -> 'TargetContract'\""`

## Maintained corroboration

### Related interface records

- [NvencAv1OpusTargetService](a-stove0-nvenc-av1-opus-target-nvencav1opustargetservice.md)

## Governing policies

- <a id="pa-8f97cee54f"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:a-stove0-nvenc-av1-opus-target:a_stove0_nvenc_av1_opus_target](../../../evidence/sources/authorities.md#src-a28a72aec7) — [some-implementations/stove0/targets/nvenc-av1-opus/target/src/a\_stove0\_nvenc\_av1\_opus\_target/\_\_init\_\_.py](../../../../../../some-implementations/stove0/targets/nvenc-av1-opus/target/src/a_stove0_nvenc_av1_opus_target/__init__.py)

### Machine authority

- `/external_contract/python/a_stove0_nvenc_av1_opus_target.NvencAv1OpusTargetService.contract`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: b54ce6f77ff38d2762abb5b9ea6057fa384bc57a326427d96a4b6f9354b72358 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'TargetContract'\""
  },
  "distribution": "a-stove0-nvenc-av1-opus-target",
  "module": "a_stove0_nvenc_av1_opus_target",
  "name": "contract",
  "owner": "a_stove0_nvenc_av1_opus_target.NvencAv1OpusTargetService",
  "unit": "member"
}
```

</details>
