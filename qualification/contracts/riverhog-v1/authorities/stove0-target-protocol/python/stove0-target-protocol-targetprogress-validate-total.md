# stove0_target_protocol.TargetProgress.validate_total

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-protocol:stove0-target-protocol-targetprogress-validate-total:6b1d3cfd5d -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-6f087f357b"></a>
- <a id="s-77a7a3f32e"></a>`distribution`: `stove0-target-protocol`
- <a id="s-264dbac989"></a>`module`: `stove0_target_protocol`
- <a id="s-f16a939750"></a>`name`: `validate_total`
- <a id="s-8ba9604268"></a>`owner`: `stove0_target_protocol.TargetProgress`
- <a id="s-e8cee9fe7d"></a>`unit`: `member`

### Declared structure

- <a id="s-886c424e09"></a>`kind`: `"method"`
- <a id="s-722eeb439d"></a>`signature`: `"\"(self) -> 'Self'\""`

## Maintained corroboration

### Related interface records

- [TargetProgress](stove0-target-protocol-targetprogress.md)

## Governing policies

- <a id="pa-27b98619ea"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-target-protocol:stove0_target_protocol](../../../evidence/sources.md#src-f4f0b22026) — [reference/stove0/packages/target-protocol/src/stove0\_target\_protocol/\_\_init\_\_.py](../../../../../../reference/stove0/packages/target-protocol/src/stove0_target_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_target_protocol.TargetProgress.validate_total`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: a480d28392010b74e53dad29592b79d1062323a4fa1b1d67aea39fce6804d5b4 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'Self'\""
  },
  "distribution": "stove0-target-protocol",
  "module": "stove0_target_protocol",
  "name": "validate_total",
  "owner": "stove0_target_protocol.TargetProgress",
  "unit": "member"
}
```

</details>
