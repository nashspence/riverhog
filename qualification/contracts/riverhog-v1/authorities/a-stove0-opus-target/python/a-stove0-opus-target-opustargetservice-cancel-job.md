# a_stove0_opus_target.OpusTargetService.cancel_job

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:a-stove0-opus-target:a-stove0-opus-target-opustargetservice-cancel-job:4c934a4697 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-stove0-opus-target](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-c4149d613d"></a>
- <a id="s-bf9fb40dd7"></a>`distribution`: `a-stove0-opus-target`
- <a id="s-2fde6b7a42"></a>`module`: `a_stove0_opus_target`
- <a id="s-abc56042ee"></a>`name`: `cancel_job`
- <a id="s-ca9aa49488"></a>`owner`: `a_stove0_opus_target.OpusTargetService`
- <a id="s-66a3a1699e"></a>`unit`: `member`

### Declared structure

- <a id="s-7d37f6f2d4"></a>`kind`: `"method"`
- <a id="s-a8438874e3"></a>`signature`: `"\"(self, job_id: 'str') -> 'TargetJobStatus'\""`

## Maintained corroboration

### Related interface records

- [OpusTargetService](a-stove0-opus-target-opustargetservice.md)

## Governing policies

- <a id="pa-86d2b5e00b"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:a-stove0-opus-target:a_stove0_opus_target](../../../evidence/sources/authorities.md#src-43f70c74af) — [some-implementations/stove0/targets/opus/target/src/a\_stove0\_opus\_target/\_\_init\_\_.py](../../../../../../some-implementations/stove0/targets/opus/target/src/a_stove0_opus_target/__init__.py)

### Machine authority

- `/external_contract/python/a_stove0_opus_target.OpusTargetService.cancel_job`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: f6a3387973df26d03beef42c6101f73fc6c57f7ae88aaeb28cb7fbb87761d0a4 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, job_id: 'str') -> 'TargetJobStatus'\""
  },
  "distribution": "a-stove0-opus-target",
  "module": "a_stove0_opus_target",
  "name": "cancel_job",
  "owner": "a_stove0_opus_target.OpusTargetService",
  "unit": "member"
}
```

</details>
