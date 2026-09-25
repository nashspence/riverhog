# a_riverhog_witness_contract_lib.CollectionWitnessStatement.sha256

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:a-riverhog-witness-contract-lib:a-riverhog-witness-contract-lib-collectio-c70c5b2f46:65f4873fab -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-witness-contract-lib](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-20d7a8b567"></a>
- <a id="s-7ae1357d8b"></a>`distribution`: `a-riverhog-witness-contract-lib`
- <a id="s-56e5c7e5fe"></a>`module`: `a_riverhog_witness_contract_lib`
- <a id="s-128e92bf7c"></a>`name`: `sha256`
- <a id="s-7ed97d5941"></a>`owner`: `a_riverhog_witness_contract_lib.CollectionWitnessStatement`
- <a id="s-4d44928164"></a>`unit`: `member`

### Declared structure

- <a id="s-4a91254a15"></a>`kind`: `"method"`
- <a id="s-90907fb252"></a>`signature`: `"\"(self) -> 'bytes'\""`

## Maintained corroboration

### Related interface records

- [CollectionWitnessStatement](a-riverhog-witness-contract-lib-collectionwitnessstatement.md)

## Governing policies

- <a id="pa-33deb83c2e"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:a-riverhog-witness-contract-lib:a_riverhog_witness_contract_lib](../../../evidence/sources/authorities.md#src-3c53e0f06b) — [some-implementations/riverhog/packages/a-riverhog-witness-contract-lib/src/a\_riverhog\_witness\_contract\_lib/\_\_init\_\_.py](../../../../../../some-implementations/riverhog/packages/a-riverhog-witness-contract-lib/src/a_riverhog_witness_contract_lib/__init__.py)

### Machine authority

- `/external_contract/python/a_riverhog_witness_contract_lib.CollectionWitnessStatement.sha256`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: b8bee5ee6b98c86e7e556d051fc89879dacb19f1f9e274f06a3c8608e906d7a3 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'bytes'\""
  },
  "distribution": "a-riverhog-witness-contract-lib",
  "module": "a_riverhog_witness_contract_lib",
  "name": "sha256",
  "owner": "a_riverhog_witness_contract_lib.CollectionWitnessStatement",
  "unit": "member"
}
```

</details>
