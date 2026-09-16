# riverhog_protocol.CollectionArtifactIdentityDocument.get

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-collectionartifactident-ffe625d558:b330f52beb -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-7888a42cd3"></a>
- <a id="s-f951385030"></a>`distribution`: `riverhog-protocol`
- <a id="s-0ea18c0086"></a>`module`: `riverhog_protocol`
- <a id="s-255fde8c0e"></a>`name`: `get`
- <a id="s-4e40c9f9ec"></a>`owner`: `riverhog_protocol.CollectionArtifactIdentityDocument`
- <a id="s-d5a22f12cc"></a>`unit`: `member`

### Declared structure

- <a id="s-da35bc93f4"></a>`kind`: `"method"`
- <a id="s-77fa51faeb"></a>`signature`: `"\"(self, key: 'str', default: 'Any' = None) -> 'Any'\""`

## Maintained corroboration

### Related interface records

- [CollectionArtifactIdentityDocument](riverhog-protocol-collectionartifactidentitydocument.md)

## Governing policies

- <a id="pa-42b8a83026"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources.md#src-19e35f15d9) — `packages/riverhog-protocol/src/riverhog_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_protocol.CollectionArtifactIdentityDocument.get`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 9e01f971ffc729a580205f3502bbf2a563ba9e476647ba864aaca39710c692d7 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, key: 'str', default: 'Any' = None) -> 'Any'\""
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "get",
  "owner": "riverhog_protocol.CollectionArtifactIdentityDocument",
  "unit": "member"
}
```

</details>
