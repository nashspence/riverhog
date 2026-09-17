# riverhog_client.create_or_resume_with_initial_collection_tags

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-create-or-resume-with-ini-9bd8a873b5:4a969273ec -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-9d74070c33"></a>
- <a id="s-35c02e4c45"></a>`distribution`: `riverhog-client`
- <a id="s-cb4c3d8964"></a>`module`: `riverhog_client`
- <a id="s-1d2be32027"></a>`name`: `create_or_resume_with_initial_collection_tags`
- <a id="s-47eb2bff32"></a>`unit`: `export`

### Declared structure

- <a id="s-739a0141f9"></a>`kind`: `"function"`
- <a id="s-dd78e2391c"></a>`signature`: `"\"(tags: 'Iterable[str]', *, create_or_resume: 'Callable[[Sequence[CollectionTag], str], Mapping[str, Any]]', add_tags: 'Callable[[int, Sequence[CollectionTag]], object]') -> 'dict[str, Any]'\""`

## Governing policies

- <a id="pa-26c71782b8"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-client:riverhog_client](../../../evidence/sources.md#src-c149020c71) — [packages/riverhog-client/src/riverhog\_client/\_\_init\_\_.py](../../../../../../packages/riverhog-client/src/riverhog_client/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_client.create_or_resume_with_initial_collection_tags`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: a4f2ee3a570c33e4c58ba0eb106de0842e504dd082cdb5a4e1f5cfd9004e1797 -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(tags: 'Iterable[str]', *, create_or_resume: 'Callable[[Sequence[CollectionTag], str], Mapping[str, Any]]', add_tags: 'Callable[[int, Sequence[CollectionTag]], object]') -> 'dict[str, Any]'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client",
  "name": "create_or_resume_with_initial_collection_tags",
  "unit": "export"
}
```

</details>
