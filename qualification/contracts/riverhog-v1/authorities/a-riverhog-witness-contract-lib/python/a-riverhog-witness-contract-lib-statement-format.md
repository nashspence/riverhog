# a_riverhog_witness_contract_lib.STATEMENT_FORMAT

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:a-riverhog-witness-contract-lib:a-riverhog-witness-contract-lib-statement-format:f9b4089e3e -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-witness-contract-lib](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-0035eaf020"></a>
- <a id="s-796c06dae5"></a>`distribution`: `a-riverhog-witness-contract-lib`
- <a id="s-2fe3bc6ffb"></a>`module`: `a_riverhog_witness_contract_lib`
- <a id="s-e39535471e"></a>`name`: `STATEMENT_FORMAT`
- <a id="s-badb647d2f"></a>`unit`: `export`

### Declared structure

- <a id="s-0e92169843"></a>`kind`: `"constant"`
- <a id="s-bfbb80e964"></a>`value`: `"a-riverhog-collection-witness/v1"`

## Governing policies

- <a id="pa-34f211148a"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:a-riverhog-witness-contract-lib:a_riverhog_witness_contract_lib](../../../evidence/sources/authorities.md#src-3c53e0f06b) — [some-implementations/riverhog/packages/a-riverhog-witness-contract-lib/src/a\_riverhog\_witness\_contract\_lib/\_\_init\_\_.py](../../../../../../some-implementations/riverhog/packages/a-riverhog-witness-contract-lib/src/a_riverhog_witness_contract_lib/__init__.py)

### Machine authority

- `/external_contract/python/a_riverhog_witness_contract_lib.STATEMENT_FORMAT`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 86ab0688507744b28a052176d76cfc35ca4d8144b4158d148ab2770217f05141 -->

```json
{
  "contract": {
    "kind": "constant",
    "value": "a-riverhog-collection-witness/v1"
  },
  "distribution": "a-riverhog-witness-contract-lib",
  "module": "a_riverhog_witness_contract_lib",
  "name": "STATEMENT_FORMAT",
  "unit": "export"
}
```

</details>
