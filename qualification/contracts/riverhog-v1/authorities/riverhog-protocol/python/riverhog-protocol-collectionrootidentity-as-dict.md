# riverhog_protocol.CollectionRootIdentity.as_dict

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-collectionrootidentity-as-dict:282d9bd21c -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-8c376ae50b"></a>
- <a id="s-9a873acf0c"></a>`distribution`: `riverhog-protocol`
- <a id="s-d862afc4d3"></a>`module`: `riverhog_protocol`
- <a id="s-169a1a29c8"></a>`name`: `as_dict`
- <a id="s-8326aff485"></a>`owner`: `riverhog_protocol.CollectionRootIdentity`
- <a id="s-839152e9d1"></a>`unit`: `member`

### Declared structure

- <a id="s-ae7fd10d8f"></a>`kind`: `"method"`
- <a id="s-7478c681ff"></a>`signature`: `"\"(self) -> 'dict[str, object]'\""`

## Maintained corroboration

### Related interface records

- [CollectionRootIdentity](riverhog-protocol-collectionrootidentity.md)

## Governing policies

- <a id="pa-50807b8910"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources.md#src-19e35f15d9) — `packages/riverhog-protocol/src/riverhog_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_protocol.CollectionRootIdentity.as_dict`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 3c15f8d64c67aa1af0bf5efb4b4b6f6fb6415a3d2055fb794b59f97e03ef3603 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'dict[str, object]'\""
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "as_dict",
  "owner": "riverhog_protocol.CollectionRootIdentity",
  "unit": "member"
}
```

</details>
