# riverhog_storage_adapter_support.FramedContent

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-storage-adapter-support:riverhog-storage-adapter-support-framedcontent:16cdad62d5 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-4944b70fb7"></a>
- <a id="s-3cf8ae5e32"></a>`distribution`: `riverhog-storage-adapter-support`
- <a id="s-01861c8af0"></a>`module`: `riverhog_storage_adapter_support`
- <a id="s-763ea6f81b"></a>`name`: `FramedContent`
- <a id="s-cda8bd0be6"></a>`unit`: `export`

### Declared structure

- <a id="s-ffba416fde"></a>`kind`: `"class"`
- <a id="s-62032b4a8c"></a>`signature`: `"\"(chunks: 'Iterator[bytes]', expected_bytes: 'int') -> 'None'\""`

## Maintained corroboration

### Related interface records

- [require_consumed](riverhog-storage-adapter-support-framedcontent-require-consumed.md)
- [__iter__](riverhog-storage-adapter-support-framedcontent-iter.md)
- [__next__](riverhog-storage-adapter-support-framedcontent-next.md)

## Governing policies

- <a id="pa-60b47da84e"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-storage-adapter-support:riverhog_storage_adapter_support](../../../evidence/sources.md#src-284271cd54) — [packages/riverhog-storage-adapter-support/src/riverhog\_storage\_adapter\_support/\_\_init\_\_.py](../../../../../../packages/riverhog-storage-adapter-support/src/riverhog_storage_adapter_support/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_storage_adapter_support.FramedContent`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: dd6ef86b5c0622a1fb74185c6a8d21bdcc8d65c5c66dac246d5501e3725faf99 -->

```json
{
  "contract": {
    "kind": "class",
    "signature": "\"(chunks: 'Iterator[bytes]', expected_bytes: 'int') -> 'None'\""
  },
  "distribution": "riverhog-storage-adapter-support",
  "module": "riverhog_storage_adapter_support",
  "name": "FramedContent",
  "unit": "export"
}
```

</details>
