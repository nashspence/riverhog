# riverhog_storage_adapter_protocol.DeleteObjectRequest.validate_revision

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-storage-adapter-protocol:riverhog-storage-adapter-protocol-deleteo-fbff60e079:25b1f39858 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-9553967ad9"></a>
- <a id="s-90118a0dbc"></a>`distribution`: `riverhog-storage-adapter-protocol`
- <a id="s-ef746151c6"></a>`module`: `riverhog_storage_adapter_protocol`
- <a id="s-b45f3f4e9f"></a>`name`: `validate_revision`
- <a id="s-fdf2d922eb"></a>`owner`: `riverhog_storage_adapter_protocol.DeleteObjectRequest`
- <a id="s-b0e3e68fcb"></a>`unit`: `member`

### Declared structure

- <a id="s-2686fb7650"></a>`kind`: `"method"`
- <a id="s-9c98cd47ff"></a>`signature`: `"\"(self) -> 'Self'\""`

## Maintained corroboration

### Related interface records

- [DeleteObjectRequest](riverhog-storage-adapter-protocol-deleteobjectrequest.md)

## Governing policies

- <a id="pa-26b29ee3d7"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-storage-adapter-protocol:riverhog_storage_adapter_protocol](../../../evidence/sources/authorities.md#src-2da8857a83) — [packages/riverhog-storage-adapter-protocol/src/riverhog\_storage\_adapter\_protocol/\_\_init\_\_.py](../../../../../../packages/riverhog-storage-adapter-protocol/src/riverhog_storage_adapter_protocol/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_storage_adapter_protocol.DeleteObjectRequest.validate_revision`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 8f0b1069584ba08ab4736c068d8d10fff67688098d96ce4b6d6d3629f1c187dc -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'Self'\""
  },
  "distribution": "riverhog-storage-adapter-protocol",
  "module": "riverhog_storage_adapter_protocol",
  "name": "validate_revision",
  "owner": "riverhog_storage_adapter_protocol.DeleteObjectRequest",
  "unit": "member"
}
```

</details>
