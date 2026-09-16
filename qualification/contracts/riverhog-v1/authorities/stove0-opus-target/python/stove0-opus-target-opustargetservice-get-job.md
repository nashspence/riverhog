# stove0_opus_target.OpusTargetService.get_job

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-opus-target:stove0-opus-target-opustargetservice-get-job:45d23e3a80 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-opus-target](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-0fd181a5d9"></a>
- <a id="s-5be6e988c1"></a>`distribution`: `stove0-opus-target`
- <a id="s-941e63713e"></a>`module`: `stove0_opus_target`
- <a id="s-73dc2f3a5d"></a>`name`: `get_job`
- <a id="s-1991469a26"></a>`owner`: `stove0_opus_target.OpusTargetService`
- <a id="s-df26241cf4"></a>`unit`: `member`

### Declared structure

- <a id="s-acec5d9c6d"></a>`kind`: `"method"`
- <a id="s-ad78d1acc0"></a>`signature`: `"\"(self, job_id: 'str') -> 'TargetJobStatus'\""`

## Maintained corroboration

### Related interface records

- [OpusTargetService](stove0-opus-target-opustargetservice.md)

## Governing policies

- <a id="pa-48dd4d36ec"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-opus-target:stove0_opus_target](../../../evidence/sources.md#src-9164f15983) — `reference/stove0/targets/opus/target/src/stove0_opus_target/__init__.py`

### Machine authority

- `/external_contract/python/stove0_opus_target.OpusTargetService.get_job`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: a528fd87b8a4ed2cff4d758900724064b73b493c819ea55910317aca358afd26 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, job_id: 'str') -> 'TargetJobStatus'\""
  },
  "distribution": "stove0-opus-target",
  "module": "stove0_opus_target",
  "name": "get_job",
  "owner": "stove0_opus_target.OpusTargetService",
  "unit": "member"
}
```
