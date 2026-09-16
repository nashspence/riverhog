# riverhog_client.transform.TransformWorkspace.resolve

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-transform-transformworkspace-resolve:1930502511 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-1e5936f70d"></a>
- <a id="s-7922793027"></a>`distribution`: `riverhog-client`
- <a id="s-1a3e51061b"></a>`module`: `riverhog_client.transform`
- <a id="s-c0fdbb6c13"></a>`name`: `resolve`
- <a id="s-a048e0ec98"></a>`owner`: `riverhog_client.transform.TransformWorkspace`
- <a id="s-ab18583afd"></a>`unit`: `member`

### Declared structure

- <a id="s-242ac5d3cb"></a>`kind`: `"method"`
- <a id="s-986c387e14"></a>`signature`: `"\"(self, relative_path: 'str') -> 'Path'\""`

## Maintained corroboration

### Related interface records

- [TransformWorkspace](riverhog-client-transform-transformworkspace.md)

## Governing policies

- <a id="pa-6bb84682c4"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-client:riverhog_client.transform](../../../evidence/sources.md#src-7a247bb534) — `packages/riverhog-client/src/riverhog_client/transform/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_client.transform.TransformWorkspace.resolve`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: ac4d771f31d344b0de7bc2e940e3cc18f7cca750b8f513b320b316688e649167 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, relative_path: 'str') -> 'Path'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client.transform",
  "name": "resolve",
  "owner": "riverhog_client.transform.TransformWorkspace",
  "unit": "member"
}
```

</details>
