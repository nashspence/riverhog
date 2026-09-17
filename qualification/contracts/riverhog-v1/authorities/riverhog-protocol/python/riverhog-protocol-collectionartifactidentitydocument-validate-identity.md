# riverhog_protocol.CollectionArtifactIdentityDocument.validate_identity

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-collectionartifactident-e6fd7422df:eec01c757e -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-621529406b"></a>
- <a id="s-6f7ec9fd2c"></a>`distribution`: `riverhog-protocol`
- <a id="s-7e2b46448d"></a>`module`: `riverhog_protocol`
- <a id="s-b88e99e5aa"></a>`name`: `validate_identity`
- <a id="s-a1238086c3"></a>`owner`: `riverhog_protocol.CollectionArtifactIdentityDocument`
- <a id="s-08bed39389"></a>`unit`: `member`

### Declared structure

- <a id="s-14388faa64"></a>`kind`: `"method"`
- <a id="s-24b3ca6b04"></a>`signature`: `"\"(self) -> 'Self'\""`

## Maintained corroboration

### Related interface records

- [CollectionArtifactIdentityDocument](riverhog-protocol-collectionartifactidentitydocument.md)

## Governing policies

- <a id="pa-2e16a442c8"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources/authorities.md#src-19e35f15d9) — [packages/riverhog-protocol/src/riverhog\_protocol/\_\_init\_\_.py](../../../../../../packages/riverhog-protocol/src/riverhog_protocol/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_protocol.CollectionArtifactIdentityDocument.validate_identity`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 427dc72e71512918e4c93f821cfb8cf13b3e39167c9ab299e718c5eb0bab22c1 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'Self'\""
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "validate_identity",
  "owner": "riverhog_protocol.CollectionArtifactIdentityDocument",
  "unit": "member"
}
```

</details>
