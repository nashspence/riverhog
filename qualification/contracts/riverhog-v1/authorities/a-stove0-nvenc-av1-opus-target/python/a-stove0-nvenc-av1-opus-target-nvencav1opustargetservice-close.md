# a_stove0_nvenc_av1_opus_target.NvencAv1OpusTargetService.close

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:a-stove0-nvenc-av1-opus-target:a-stove0-nvenc-av1-opus-target-nvencav1op-f743803f8b:c9cef1f06b -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-stove0-nvenc-av1-opus-target](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-3908f0af95"></a>
- <a id="s-a14439d4bd"></a>`distribution`: `a-stove0-nvenc-av1-opus-target`
- <a id="s-2f4c1a584e"></a>`module`: `a_stove0_nvenc_av1_opus_target`
- <a id="s-88ae09f14f"></a>`name`: `close`
- <a id="s-42a8a57d92"></a>`owner`: `a_stove0_nvenc_av1_opus_target.NvencAv1OpusTargetService`
- <a id="s-c9aa0e346b"></a>`unit`: `member`

### Declared structure

- <a id="s-b2630e34cc"></a>`kind`: `"method"`
- <a id="s-144945320d"></a>`signature`: `"\"(self) -> 'None'\""`

## Maintained corroboration

### Related interface records

- [NvencAv1OpusTargetService](a-stove0-nvenc-av1-opus-target-nvencav1opustargetservice.md)

## Governing policies

- <a id="pa-e680d5f52a"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:a-stove0-nvenc-av1-opus-target:a_stove0_nvenc_av1_opus_target](../../../evidence/sources/authorities.md#src-a28a72aec7) — [some-implementations/stove0/targets/nvenc-av1-opus/target/src/a\_stove0\_nvenc\_av1\_opus\_target/\_\_init\_\_.py](../../../../../../some-implementations/stove0/targets/nvenc-av1-opus/target/src/a_stove0_nvenc_av1_opus_target/__init__.py)

### Machine authority

- `/external_contract/python/a_stove0_nvenc_av1_opus_target.NvencAv1OpusTargetService.close`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 4931d642300a9b0b26ec1a416ba1127cfbc999dbe89248b682c4cc764e1c83e3 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'None'\""
  },
  "distribution": "a-stove0-nvenc-av1-opus-target",
  "module": "a_stove0_nvenc_av1_opus_target",
  "name": "close",
  "owner": "a_stove0_nvenc_av1_opus_target.NvencAv1OpusTargetService",
  "unit": "member"
}
```

</details>
