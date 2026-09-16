# riverhog_protocol.CollectionDescriptionDocument.seal

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-collectiondescriptiondocument-seal:a3cf5561c0 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-0c80038124"></a>
- <a id="s-6fa901de43"></a>`distribution`: `riverhog-protocol`
- <a id="s-521d201239"></a>`module`: `riverhog_protocol`
- <a id="s-3343c23b45"></a>`name`: `seal`
- <a id="s-0da03aebaf"></a>`owner`: `riverhog_protocol.CollectionDescriptionDocument`
- <a id="s-13b41a1d14"></a>`unit`: `member`

### Declared structure

- <a id="s-ee89b6e6d2"></a>`kind`: `"classmethod"`
- <a id="s-615bf023b1"></a>`signature`: `"\"(cls, *, archive_root_sha256: 'str', revision: 'int', description: 'str \| None') -> 'CollectionDescriptionDocument'\""`

## Maintained corroboration

### Related interface records

- [CollectionDescriptionDocument](riverhog-protocol-collectiondescriptiondocument.md)

## Governing policies

- <a id="pa-64f08be638"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources.md#src-19e35f15d9) — `packages/riverhog-protocol/src/riverhog_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_protocol.CollectionDescriptionDocument.seal`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 13e5e50c3f062c45239008763139fa004db577ae57f0f30dc27eea839d2537b4 -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, *, archive_root_sha256: 'str', revision: 'int', description: 'str | None') -> 'CollectionDescriptionDocument'\""
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "seal",
  "owner": "riverhog_protocol.CollectionDescriptionDocument",
  "unit": "member"
}
```

</details>
