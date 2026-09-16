# riverhog_protocol.error_type_for_code

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-error-type-for-code:2bb07b196f -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-5166a7345a"></a>
- <a id="s-12684591cf"></a>`distribution`: `riverhog-protocol`
- <a id="s-a7c51e6c46"></a>`module`: `riverhog_protocol`
- <a id="s-93136f3c6a"></a>`name`: `error_type_for_code`
- <a id="s-0f88c1c3e7"></a>`unit`: `export`

### Declared structure

- <a id="s-8ccd952a4a"></a>`kind`: `"function"`
- <a id="s-3b2212f4ea"></a>`signature`: `"\"(code: 'str') -> 'type[RiverhogError] \| None'\""`

## Governing policies

- <a id="pa-b551fa5a86"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources.md#src-19e35f15d9) — `packages/riverhog-protocol/src/riverhog_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_protocol.error_type_for_code`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 2f256a85ee899864154a69c45efc749ac9579ec75f99d17b3ac48cbcba1f41d3 -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(code: 'str') -> 'type[RiverhogError] | None'\""
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "error_type_for_code",
  "unit": "export"
}
```

</details>
