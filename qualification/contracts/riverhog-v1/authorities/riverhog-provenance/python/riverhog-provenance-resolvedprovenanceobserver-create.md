# riverhog_provenance.ResolvedProvenanceObserver.create

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-provenance:riverhog-provenance-resolvedprovenanceobs-315410b096:f659c69cea -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-provenance](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-560d9902f5"></a>
- <a id="s-0afab3d478"></a>`distribution`: `riverhog-provenance`
- <a id="s-17fccb21a9"></a>`module`: `riverhog_provenance`
- <a id="s-adfa826141"></a>`name`: `create`
- <a id="s-5ea5dcc783"></a>`owner`: `riverhog_provenance.ResolvedProvenanceObserver`
- <a id="s-ed916f3cef"></a>`unit`: `member`

### Declared structure

- <a id="s-d7fb2a913b"></a>`kind`: `"method"`
- <a id="s-250c45c599"></a>`signature`: `"\"(self) -> 'FileStateObserver'\""`

## Maintained corroboration

### Related interface records

- [riverhog_provenance.ResolvedProvenanceObserver](riverhog-provenance-resolvedprovenanceobserver.md)

## Governing policies

- <a id="pa-0c4f88aaab"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-provenance:riverhog_provenance](../../../evidence/sources.md#src-38ef3a6054) — `packages/riverhog-provenance/src/riverhog_provenance/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_provenance.ResolvedProvenanceObserver.create`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: ac7e32cb88c848395dda34dc5c6a1f6eb2c7dad7646b95a5303a7ccc1abfd0bc -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'FileStateObserver'\""
  },
  "distribution": "riverhog-provenance",
  "module": "riverhog_provenance",
  "name": "create",
  "owner": "riverhog_provenance.ResolvedProvenanceObserver",
  "unit": "member"
}
```
