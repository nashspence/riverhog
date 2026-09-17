# riverhog_provenance.PROVENANCE_OBSERVER_REFERENCE_FORMAT

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-provenance:riverhog-provenance-provenance-observer-r-a2ddb80455:9d08491a33 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-provenance](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-30ea86094a"></a>
- <a id="s-44fe3b5c70"></a>`distribution`: `riverhog-provenance`
- <a id="s-9b7cd41773"></a>`module`: `riverhog_provenance`
- <a id="s-5af7bc355d"></a>`name`: `PROVENANCE_OBSERVER_REFERENCE_FORMAT`
- <a id="s-468ea359bf"></a>`unit`: `export`

### Declared structure

- <a id="s-4c96dfb562"></a>`kind`: `"constant"`
- <a id="s-4ef46dd90f"></a>`value`: `"riverhog-provenance-observer-reference/v1"`

## Governing policies

- <a id="pa-5a8de6be41"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-provenance:riverhog_provenance](../../../evidence/sources/authorities.md#src-38ef3a6054) — [packages/riverhog-provenance/src/riverhog\_provenance/\_\_init\_\_.py](../../../../../../packages/riverhog-provenance/src/riverhog_provenance/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_provenance.PROVENANCE_OBSERVER_REFERENCE_FORMAT`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: e2b0de55a4f20ad861d70faab2d528f06dc63f6726c33492511da37d2d41199c -->

```json
{
  "contract": {
    "kind": "constant",
    "value": "riverhog-provenance-observer-reference/v1"
  },
  "distribution": "riverhog-provenance",
  "module": "riverhog_provenance",
  "name": "PROVENANCE_OBSERVER_REFERENCE_FORMAT",
  "unit": "export"
}
```

</details>
