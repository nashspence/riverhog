# riverhog_protocol.ProcessingClaimPlanSealDocument.__getitem__

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-processingclaimplanseal-3052977c36:c53e7054a4 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-828ba72d79"></a>
- <a id="s-2777f0141b"></a>`distribution`: `riverhog-protocol`
- <a id="s-1dc08274fa"></a>`module`: `riverhog_protocol`
- <a id="s-6512a0e2c4"></a>`name`: `__getitem__`
- <a id="s-8e737147f6"></a>`owner`: `riverhog_protocol.ProcessingClaimPlanSealDocument`
- <a id="s-fbbc9f95ed"></a>`unit`: `member`

### Declared structure

- <a id="s-c0121fff39"></a>`kind`: `"method"`
- <a id="s-a53d6e4c73"></a>`signature`: `"\"(self, key: 'str') -> 'Any'\""`

## Maintained corroboration

### Related interface records

- [ProcessingClaimPlanSealDocument](riverhog-protocol-processingclaimplansealdocument.md)

## Governing policies

- <a id="pa-6e02efd08e"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources/authorities.md#src-19e35f15d9) — [packages/riverhog-protocol/src/riverhog\_protocol/\_\_init\_\_.py](../../../../../../packages/riverhog-protocol/src/riverhog_protocol/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_protocol.ProcessingClaimPlanSealDocument.__getitem__`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: d0eb19e12e128a5c67f4bbf8dffeddd81ccf32391ad154296152b9b7454733e3 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, key: 'str') -> 'Any'\""
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "__getitem__",
  "owner": "riverhog_protocol.ProcessingClaimPlanSealDocument",
  "unit": "member"
}
```

</details>
