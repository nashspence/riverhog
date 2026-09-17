# stove0_opus_target.OpusTargetService.put_job

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-opus-target:stove0-opus-target-opustargetservice-put-job:8b38257e54 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-opus-target](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-b586f5ec2f"></a>
- <a id="s-cf50d458fb"></a>`distribution`: `stove0-opus-target`
- <a id="s-1120c38e4b"></a>`module`: `stove0_opus_target`
- <a id="s-d157bc1d92"></a>`name`: `put_job`
- <a id="s-d23d565c31"></a>`owner`: `stove0_opus_target.OpusTargetService`
- <a id="s-630b121e85"></a>`unit`: `member`

### Declared structure

- <a id="s-0523beb928"></a>`kind`: `"method"`
- <a id="s-e4219d47db"></a>`signature`: `"\"(self, request: 'TargetJobRequest') -> 'TargetJobStatus'\""`

## Maintained corroboration

### Related interface records

- [OpusTargetService](stove0-opus-target-opustargetservice.md)

## Governing policies

- <a id="pa-bb7d14e21f"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-opus-target:stove0_opus_target](../../../evidence/sources.md#src-9164f15983) — [reference/stove0/targets/opus/target/src/stove0\_opus\_target/\_\_init\_\_.py](../../../../../../reference/stove0/targets/opus/target/src/stove0_opus_target/__init__.py)

### Machine authority

- `/external_contract/python/stove0_opus_target.OpusTargetService.put_job`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 8ec4f7d731897b260d3f9e7468e9f3ec25b94eac194bb3150a25e2bea555d233 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, request: 'TargetJobRequest') -> 'TargetJobStatus'\""
  },
  "distribution": "stove0-opus-target",
  "module": "stove0_opus_target",
  "name": "put_job",
  "owner": "stove0_opus_target.OpusTargetService",
  "unit": "member"
}
```

</details>
