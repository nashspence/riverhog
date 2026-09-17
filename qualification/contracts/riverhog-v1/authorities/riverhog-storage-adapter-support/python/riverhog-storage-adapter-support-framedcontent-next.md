# riverhog_storage_adapter_support.FramedContent.__next__

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-storage-adapter-support:riverhog-storage-adapter-support-framedcontent-next:ceb496edac -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-b67cead1dd"></a>
- <a id="s-1a33f1861a"></a>`distribution`: `riverhog-storage-adapter-support`
- <a id="s-9ab2908e7f"></a>`module`: `riverhog_storage_adapter_support`
- <a id="s-32a89f9224"></a>`name`: `__next__`
- <a id="s-658da4f503"></a>`owner`: `riverhog_storage_adapter_support.FramedContent`
- <a id="s-001190cc2e"></a>`unit`: `member`

### Declared structure

- <a id="s-d9153251ae"></a>`kind`: `"method"`
- <a id="s-9f36bfa46f"></a>`signature`: `"\"(self) -> 'bytes'\""`

## Maintained corroboration

### Related interface records

- [FramedContent](riverhog-storage-adapter-support-framedcontent.md)

## Governing policies

- <a id="pa-1eac8b4cc1"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-storage-adapter-support:riverhog_storage_adapter_support](../../../evidence/sources/authorities.md#src-284271cd54) — [packages/riverhog-storage-adapter-support/src/riverhog\_storage\_adapter\_support/\_\_init\_\_.py](../../../../../../packages/riverhog-storage-adapter-support/src/riverhog_storage_adapter_support/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_storage_adapter_support.FramedContent.__next__`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 0e71a559930976daee68deebab43f55ae60a6038532df1f50b1b46e94d369540 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'bytes'\""
  },
  "distribution": "riverhog-storage-adapter-support",
  "module": "riverhog_storage_adapter_support",
  "name": "__next__",
  "owner": "riverhog_storage_adapter_support.FramedContent",
  "unit": "member"
}
```

</details>
