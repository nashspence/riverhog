# riverhog_protocol.ApplicationSort

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-applicationsort:cc338ff70c -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-65ce6bc3ee"></a>
- <a id="s-5b628c194e"></a>`distribution`: `riverhog-protocol`
- <a id="s-f230013fdd"></a>`module`: `riverhog_protocol`
- <a id="s-6a034556a0"></a>`name`: `ApplicationSort`
- <a id="s-f22fb3d56c"></a>`unit`: `export`

### Declared structure

- <a id="s-661e7e508f"></a>`kind`: `"type-alias"`
- <a id="s-a34aeadade"></a>`value`: `"typing.Literal['name', 'keys', 'active_keys', 'last_used_at']"`

## Governing policies

- <a id="pa-19432af2cb"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources.md#src-19e35f15d9) — `packages/riverhog-protocol/src/riverhog_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_protocol.ApplicationSort`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: dc065dd5a3426f29fc38a98e51b4289fdc5d1bc6c761a50a64294d02066a3139 -->

```json
{
  "contract": {
    "kind": "type-alias",
    "value": "typing.Literal['name', 'keys', 'active_keys', 'last_used_at']"
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "ApplicationSort",
  "unit": "export"
}
```

</details>
