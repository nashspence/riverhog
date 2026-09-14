# stove0_observer_protocol.SHA256_PATTERN

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-observer-protocol:stove0-observer-protocol-sha256-pattern:e4234532e6 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-observer-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-0e958c821a"></a>
- <a id="s-a7e87e05be"></a>`distribution`: `stove0-observer-protocol`
- <a id="s-d79d7a11c9"></a>`module`: `stove0_observer_protocol`
- <a id="s-f35792a5a8"></a>`name`: `SHA256_PATTERN`
- <a id="s-890cafe736"></a>`unit`: `export`

### Declared structure

- <a id="s-d889e7ebd2"></a>`kind`: `"constant"`
- <a id="s-2f24fc5887"></a>`value`: `"^[0-9a-f]{64}$"`

## Governing policies

- <a id="pa-4026d7bff5"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-observer-protocol:stove0_observer_protocol](../../../evidence/sources.md#src-62450e0156) — `reference/stove0/packages/observer-protocol/src/stove0_observer_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_observer_protocol.SHA256_PATTERN`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 6fe58529a4456f9b361f8702a54754c1082fa20f8bbede607c4a1baca1ad1035 -->

```json
{
  "contract": {
    "kind": "constant",
    "value": "^[0-9a-f]{64}$"
  },
  "distribution": "stove0-observer-protocol",
  "module": "stove0_observer_protocol",
  "name": "SHA256_PATTERN",
  "unit": "export"
}
```
