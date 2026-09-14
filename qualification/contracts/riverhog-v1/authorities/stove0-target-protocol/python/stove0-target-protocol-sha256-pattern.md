# stove0_target_protocol.SHA256_PATTERN

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-protocol:stove0-target-protocol-sha256-pattern:db87a1837d -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-176db117a0"></a>
- <a id="s-f994426962"></a>`distribution`: `stove0-target-protocol`
- <a id="s-d888a44862"></a>`module`: `stove0_target_protocol`
- <a id="s-a9dfbe7bf5"></a>`name`: `SHA256_PATTERN`
- <a id="s-7f66c4d41d"></a>`unit`: `export`

### Declared structure

- <a id="s-a1b265f33e"></a>`kind`: `"constant"`
- <a id="s-2c9d1be26b"></a>`value`: `"^[0-9a-f]{64}$"`

## Governing policies

- <a id="pa-585f04e052"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-target-protocol:stove0_target_protocol](../../../evidence/sources.md#src-f4f0b22026) — `reference/stove0/packages/target-protocol/src/stove0_target_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_target_protocol.SHA256_PATTERN`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: e8ed222aeb388bfbc58cc6d7e7a609a4de630ca3eb9a72985c41a06b5392a1ad -->

```json
{
  "contract": {
    "kind": "constant",
    "value": "^[0-9a-f]{64}$"
  },
  "distribution": "stove0-target-protocol",
  "module": "stove0_target_protocol",
  "name": "SHA256_PATTERN",
  "unit": "export"
}
```
