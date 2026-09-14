# stove0_target_protocol.ARTIFACT_ID_PATTERN

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-protocol:stove0-target-protocol-artifact-id-pattern:cb459ef68a -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-54d8fd516b"></a>
- <a id="s-46af6168a5"></a>`distribution`: `stove0-target-protocol`
- <a id="s-cc66ba4b01"></a>`module`: `stove0_target_protocol`
- <a id="s-1261890c7c"></a>`name`: `ARTIFACT_ID_PATTERN`
- <a id="s-a962225927"></a>`unit`: `export`

### Declared structure

- <a id="s-03e51b01a3"></a>`kind`: `"constant"`
- <a id="s-5579478925"></a>`value`: `"^[A-Za-z0-9]&#40;?:[A-Za-z0-9._-]{0,158}[A-Za-z0-9])?$"`

## Governing policies

- <a id="pa-b37a46bdca"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-target-protocol:stove0_target_protocol](../../../evidence/sources.md#src-f4f0b22026) — `reference/stove0/packages/target-protocol/src/stove0_target_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_target_protocol.ARTIFACT_ID_PATTERN`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 98af61e3d2eb8a4a6d1aaab1877bb7b17ce4811bbd29156ad0e87907ada7166a -->

```json
{
  "contract": {
    "kind": "constant",
    "value": "^[A-Za-z0-9]\u0028?:[A-Za-z0-9._-]{0,158}[A-Za-z0-9])?$"
  },
  "distribution": "stove0-target-protocol",
  "module": "stove0_target_protocol",
  "name": "ARTIFACT_ID_PATTERN",
  "unit": "export"
}
```
