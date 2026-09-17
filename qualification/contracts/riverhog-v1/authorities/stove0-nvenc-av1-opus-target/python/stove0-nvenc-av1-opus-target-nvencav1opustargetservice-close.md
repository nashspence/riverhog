# stove0_nvenc_av1_opus_target.NvencAv1OpusTargetService.close

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-nvenc-av1-opus-target:stove0-nvenc-av1-opus-target-nvencav1opus-f16f26ab84:597eb98816 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-nvenc-av1-opus-target](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-d96b7c0155"></a>
- <a id="s-54d7bf86aa"></a>`distribution`: `stove0-nvenc-av1-opus-target`
- <a id="s-1ee9038ed6"></a>`module`: `stove0_nvenc_av1_opus_target`
- <a id="s-5f120be3fd"></a>`name`: `close`
- <a id="s-192da0a267"></a>`owner`: `stove0_nvenc_av1_opus_target.NvencAv1OpusTargetService`
- <a id="s-1f147ced2c"></a>`unit`: `member`

### Declared structure

- <a id="s-3a9956873b"></a>`kind`: `"method"`
- <a id="s-c733948fbf"></a>`signature`: `"\"(self) -> 'None'\""`

## Maintained corroboration

### Related interface records

- [NvencAv1OpusTargetService](stove0-nvenc-av1-opus-target-nvencav1opustargetservice.md)

## Governing policies

- <a id="pa-9e5050c201"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-nvenc-av1-opus-target:stove0_nvenc_av1_opus_target](../../../evidence/sources/authorities.md#src-b6e7b93ef1) — [reference/stove0/targets/nvenc-av1-opus/target/src/stove0\_nvenc\_av1\_opus\_target/\_\_init\_\_.py](../../../../../../reference/stove0/targets/nvenc-av1-opus/target/src/stove0_nvenc_av1_opus_target/__init__.py)

### Machine authority

- `/external_contract/python/stove0_nvenc_av1_opus_target.NvencAv1OpusTargetService.close`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 9256c5c0dc37a633578f1d3452b8e3366404ba35b9a412824294802e28dddc5c -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'None'\""
  },
  "distribution": "stove0-nvenc-av1-opus-target",
  "module": "stove0_nvenc_av1_opus_target",
  "name": "close",
  "owner": "stove0_nvenc_av1_opus_target.NvencAv1OpusTargetService",
  "unit": "member"
}
```

</details>
