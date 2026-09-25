# a_riverhog_witness_contract_lib.CollectionWitnessStatement.serialize

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:a-riverhog-witness-contract-lib:a-riverhog-witness-contract-lib-collectio-8cf0de34e6:541f5ec481 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-witness-contract-lib](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-b8cc3b302c"></a>
- <a id="s-8f83e7e475"></a>`distribution`: `a-riverhog-witness-contract-lib`
- <a id="s-ac32bbeeb1"></a>`module`: `a_riverhog_witness_contract_lib`
- <a id="s-9dbb60bbc8"></a>`name`: `serialize`
- <a id="s-68a1032eed"></a>`owner`: `a_riverhog_witness_contract_lib.CollectionWitnessStatement`
- <a id="s-31ef8f4203"></a>`unit`: `member`

### Declared structure

- <a id="s-7998298a6c"></a>`kind`: `"method"`
- <a id="s-92ea9165d0"></a>`signature`: `"\"(self) -> 'bytes'\""`

## Maintained corroboration

### Related interface records

- [CollectionWitnessStatement](a-riverhog-witness-contract-lib-collectionwitnessstatement.md)

## Governing policies

- <a id="pa-27463ddaeb"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:a-riverhog-witness-contract-lib:a_riverhog_witness_contract_lib](../../../evidence/sources/authorities.md#src-3c53e0f06b) — [some-implementations/riverhog/packages/a-riverhog-witness-contract-lib/src/a\_riverhog\_witness\_contract\_lib/\_\_init\_\_.py](../../../../../../some-implementations/riverhog/packages/a-riverhog-witness-contract-lib/src/a_riverhog_witness_contract_lib/__init__.py)

### Machine authority

- `/external_contract/python/a_riverhog_witness_contract_lib.CollectionWitnessStatement.serialize`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 1b31630e2d41ea911e7b9999e54b7cc4388f420606e8d7e7663b2e794e7e9545 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'bytes'\""
  },
  "distribution": "a-riverhog-witness-contract-lib",
  "module": "a_riverhog_witness_contract_lib",
  "name": "serialize",
  "owner": "a_riverhog_witness_contract_lib.CollectionWitnessStatement",
  "unit": "member"
}
```

</details>
