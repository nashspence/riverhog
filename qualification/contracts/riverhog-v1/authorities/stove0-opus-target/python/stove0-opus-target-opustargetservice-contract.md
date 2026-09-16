# stove0_opus_target.OpusTargetService.contract

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-opus-target:stove0-opus-target-opustargetservice-contract:24e7bc45a3 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-opus-target](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-a7dbe2fcbb"></a>
- <a id="s-0a1e36babc"></a>`distribution`: `stove0-opus-target`
- <a id="s-18ad16c737"></a>`module`: `stove0_opus_target`
- <a id="s-9ce6b49d3f"></a>`name`: `contract`
- <a id="s-40ad9d47b7"></a>`owner`: `stove0_opus_target.OpusTargetService`
- <a id="s-bec993b590"></a>`unit`: `member`

### Declared structure

- <a id="s-0d6f0cfbe1"></a>`kind`: `"method"`
- <a id="s-1d60f80794"></a>`signature`: `"\"(self) -> 'TargetContract'\""`

## Maintained corroboration

### Related interface records

- [OpusTargetService](stove0-opus-target-opustargetservice.md)

## Governing policies

- <a id="pa-2b4677ad10"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-opus-target:stove0_opus_target](../../../evidence/sources.md#src-9164f15983) — `reference/stove0/targets/opus/target/src/stove0_opus_target/__init__.py`

### Machine authority

- `/external_contract/python/stove0_opus_target.OpusTargetService.contract`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 55bf49cd500752ef2fcca2366c9745dcdc9294ec95bae8ec94a3efb7079f74a1 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'TargetContract'\""
  },
  "distribution": "stove0-opus-target",
  "module": "stove0_opus_target",
  "name": "contract",
  "owner": "stove0_opus_target.OpusTargetService",
  "unit": "member"
}
```
