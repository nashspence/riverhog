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
| Field | Shape |
|---|---|
| <a id="s-f5754fd8cb"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-3cf8ae5e32"></a>`distribution` | "riverhog-storage-adapter-support" |
| <a id="s-01861c8af0"></a>`module` | "riverhog_storage_adapter_support" |
| <a id="s-763ea6f81b"></a>`name` | "FramedContent" |
| <a id="s-cda8bd0be6"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [riverhog_storage_adapter_support.FramedContent.require_consumed](riverhog-storage-adapter-support-framedcontent-require-consumed.md)
- [riverhog_storage_adapter_support.FramedContent.__iter__](riverhog-storage-adapter-support-framedcontent-iter.md)
- [riverhog_storage_adapter_support.FramedContent.__next__](riverhog-storage-adapter-support-framedcontent-next.md)

## Governing policies

- <a id="pa-60b47da84e"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-storage-adapter-support:riverhog_storage_adapter_support](../../../evidence/sources.md#src-284271cd54) — `packages/riverhog-storage-adapter-support/src/riverhog_storage_adapter_support/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_storage_adapter_support.FramedContent`

### Exact owned JSON

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
