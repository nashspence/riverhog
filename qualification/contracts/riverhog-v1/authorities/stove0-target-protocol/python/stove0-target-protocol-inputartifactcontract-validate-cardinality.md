# stove0_target_protocol.InputArtifactContract.validate_cardinality

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-protocol:stove0-target-protocol-inputartifactcontr-c13cf0a0c5:8adc290dd8 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-442eb71198"></a>
- <a id="s-cdf4604ecd"></a>`distribution`: `stove0-target-protocol`
- <a id="s-fd0949c5b7"></a>`module`: `stove0_target_protocol`
- <a id="s-8f6ba00e04"></a>`name`: `validate_cardinality`
- <a id="s-fdef2b2fe5"></a>`owner`: `stove0_target_protocol.InputArtifactContract`
- <a id="s-c321b81f4d"></a>`unit`: `member`

### Declared structure

- <a id="s-04f0a31b36"></a>`kind`: `"method"`
- <a id="s-0715ddd19a"></a>`signature`: `"\"(self) -> 'Self'\""`

## Maintained corroboration

### Related interface records

- [InputArtifactContract](stove0-target-protocol-inputartifactcontract.md)

## Governing policies

- <a id="pa-a993e271e2"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-target-protocol:stove0_target_protocol](../../../evidence/sources.md#src-f4f0b22026) — `reference/stove0/packages/target-protocol/src/stove0_target_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_target_protocol.InputArtifactContract.validate_cardinality`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: af1bd57d9d24e4d4e56859ce917b1f3092b93e36d01c789db495b05165a4d726 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'Self'\""
  },
  "distribution": "stove0-target-protocol",
  "module": "stove0_target_protocol",
  "name": "validate_cardinality",
  "owner": "stove0_target_protocol.InputArtifactContract",
  "unit": "member"
}
```

</details>
