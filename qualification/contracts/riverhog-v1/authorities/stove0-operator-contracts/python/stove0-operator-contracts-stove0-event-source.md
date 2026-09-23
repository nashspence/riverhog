# stove0_operator_contracts.STOVE0_EVENT_SOURCE

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-operator-contracts:stove0-operator-contracts-stove0-event-source:0eee1e1b39 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-operator-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-0f49f4d6e0"></a>
- <a id="s-27c3044850"></a>`distribution`: `stove0-operator-contracts`
- <a id="s-89b10c1a75"></a>`module`: `stove0_operator_contracts`
- <a id="s-0a3644b961"></a>`name`: `STOVE0_EVENT_SOURCE`
- <a id="s-7834be6d93"></a>`unit`: `export`

### Declared structure

- <a id="s-f8c634f4fa"></a>`kind`: `"constant"`
- <a id="s-1671560a8d"></a>`value`: `"urn:riverhog:stove0"`

## Governing policies

- <a id="pa-f552c9a6f1"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-operator-contracts:stove0_operator_contracts](../../../evidence/sources/authorities.md#src-51ad84528d) — [some-implementations/stove0/packages/operator-contracts/src/stove0\_operator\_contracts/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/operator-contracts/src/stove0_operator_contracts/__init__.py)

### Machine authority

- `/external_contract/python/stove0_operator_contracts.STOVE0_EVENT_SOURCE`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: fd771c6ed892eba362ddcb458cf11adb5a01f6b8c18194dca905a97f9a63d0c2 -->

```json
{
  "contract": {
    "kind": "constant",
    "value": "urn:riverhog:stove0"
  },
  "distribution": "stove0-operator-contracts",
  "module": "stove0_operator_contracts",
  "name": "STOVE0_EVENT_SOURCE",
  "unit": "export"
}
```

</details>
