# riverhog_protocol.ArtifactDisposition.as_dict

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-artifactdisposition-as-dict:68c8327a21 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-a2d66737c2"></a>
- <a id="s-705bb9aaa2"></a>`distribution`: `riverhog-protocol`
- <a id="s-17dc53995d"></a>`module`: `riverhog_protocol`
- <a id="s-f07da4f176"></a>`name`: `as_dict`
- <a id="s-0b6388f167"></a>`owner`: `riverhog_protocol.ArtifactDisposition`
- <a id="s-ddefee5310"></a>`unit`: `member`

### Declared structure

- <a id="s-1f67f9e651"></a>`kind`: `"method"`
- <a id="s-b94803ef6e"></a>`signature`: `"\"(self) -> 'dict[str, object]'\""`

## Maintained corroboration

### Related interface records

- [ArtifactDisposition](riverhog-protocol-artifactdisposition.md)

## Governing policies

- <a id="pa-b5ffe35a6e"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources.md#src-19e35f15d9) — `packages/riverhog-protocol/src/riverhog_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_protocol.ArtifactDisposition.as_dict`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: da024c4568c5fdb2f53da8abef2b5cef522ee2443409d83ba8abdc6c9c11526c -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'dict[str, object]'\""
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "as_dict",
  "owner": "riverhog_protocol.ArtifactDisposition",
  "unit": "member"
}
```
