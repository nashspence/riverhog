# stove0_target_protocol.update_target_output_binding_commitment

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-protocol:stove0-target-protocol-update-target-outp-fc99c197f2:1416446061 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-1e84e5a637"></a>
- <a id="s-7f364ec2cb"></a>`distribution`: `stove0-target-protocol`
- <a id="s-4e7714241c"></a>`module`: `stove0_target_protocol`
- <a id="s-9481967648"></a>`name`: `update_target_output_binding_commitment`
- <a id="s-666d1a5091"></a>`unit`: `export`

### Declared structure

- <a id="s-cbc6ffa971"></a>`kind`: `"function"`
- <a id="s-846fd666b8"></a>`signature`: `"\"(digest: 'Any', *, ordinal: 'int', binding: 'TargetOutputBinding') -> 'None'\""`

## Governing policies

- <a id="pa-eb2b2eb222"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-target-protocol:stove0_target_protocol](../../../evidence/sources.md#src-f4f0b22026) — `reference/stove0/packages/target-protocol/src/stove0_target_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_target_protocol.update_target_output_binding_commitment`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 77b33b601fd48d805178162757f3151621db54895f086a45ccc0b3c3843893b8 -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(digest: 'Any', *, ordinal: 'int', binding: 'TargetOutputBinding') -> 'None'\""
  },
  "distribution": "stove0-target-protocol",
  "module": "stove0_target_protocol",
  "name": "update_target_output_binding_commitment",
  "unit": "export"
}
```
