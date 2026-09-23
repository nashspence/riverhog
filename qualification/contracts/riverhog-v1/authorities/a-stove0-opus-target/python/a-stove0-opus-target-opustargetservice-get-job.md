# a_stove0_opus_target.OpusTargetService.get_job

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:a-stove0-opus-target:a-stove0-opus-target-opustargetservice-get-job:672287cbb1 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-stove0-opus-target](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-d8312ad5c3"></a>
- <a id="s-c95b5710ca"></a>`distribution`: `a-stove0-opus-target`
- <a id="s-b949db4773"></a>`module`: `a_stove0_opus_target`
- <a id="s-bbd9da3d25"></a>`name`: `get_job`
- <a id="s-4c89ea0ba7"></a>`owner`: `a_stove0_opus_target.OpusTargetService`
- <a id="s-5a348d464f"></a>`unit`: `member`

### Declared structure

- <a id="s-89ede86b57"></a>`kind`: `"method"`
- <a id="s-321bb19a6e"></a>`signature`: `"\"(self, job_id: 'str') -> 'TargetJobStatus'\""`

## Maintained corroboration

### Related interface records

- [OpusTargetService](a-stove0-opus-target-opustargetservice.md)

## Governing policies

- <a id="pa-05c277b58f"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:a-stove0-opus-target:a_stove0_opus_target](../../../evidence/sources/authorities.md#src-43f70c74af) — [some-implementations/stove0/targets/opus/target/src/a\_stove0\_opus\_target/\_\_init\_\_.py](../../../../../../some-implementations/stove0/targets/opus/target/src/a_stove0_opus_target/__init__.py)

### Machine authority

- `/external_contract/python/a_stove0_opus_target.OpusTargetService.get_job`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: f773098028a39ed9831ead4dad3617e997923060cd1592ae2e8b1e78696241c4 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, job_id: 'str') -> 'TargetJobStatus'\""
  },
  "distribution": "a-stove0-opus-target",
  "module": "a_stove0_opus_target",
  "name": "get_job",
  "owner": "a_stove0_opus_target.OpusTargetService",
  "unit": "member"
}
```

</details>
