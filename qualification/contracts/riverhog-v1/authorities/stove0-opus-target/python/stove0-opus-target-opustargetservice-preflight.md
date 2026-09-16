# stove0_opus_target.OpusTargetService.preflight

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-opus-target:stove0-opus-target-opustargetservice-preflight:1861acddac -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-opus-target](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-0fe9f2935c"></a>
- <a id="s-e2b03ffc29"></a>`distribution`: `stove0-opus-target`
- <a id="s-49f704fc53"></a>`module`: `stove0_opus_target`
- <a id="s-c2f23c14f4"></a>`name`: `preflight`
- <a id="s-113a301d4b"></a>`owner`: `stove0_opus_target.OpusTargetService`
- <a id="s-4044a1f4fc"></a>`unit`: `member`

### Declared structure

- <a id="s-6472ac3b22"></a>`kind`: `"method"`
- <a id="s-c5e4dd7ac9"></a>`signature`: `"\"(self, request: 'TargetPreflightRequest') -> 'TargetPreflightResponse'\""`

## Maintained corroboration

### Related interface records

- [OpusTargetService](stove0-opus-target-opustargetservice.md)

## Governing policies

- <a id="pa-a3a703190f"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-opus-target:stove0_opus_target](../../../evidence/sources.md#src-9164f15983) — `reference/stove0/targets/opus/target/src/stove0_opus_target/__init__.py`

### Machine authority

- `/external_contract/python/stove0_opus_target.OpusTargetService.preflight`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: e8565df6ae69e9e34126a0a0aa5a69d0f1a95f8ec06af0f8cfdf2af00e0771e2 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, request: 'TargetPreflightRequest') -> 'TargetPreflightResponse'\""
  },
  "distribution": "stove0-opus-target",
  "module": "stove0_opus_target",
  "name": "preflight",
  "owner": "stove0_opus_target.OpusTargetService",
  "unit": "member"
}
```

</details>
