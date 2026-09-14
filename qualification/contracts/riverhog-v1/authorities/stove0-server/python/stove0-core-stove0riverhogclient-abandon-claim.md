# stove0_core.Stove0RiverhogClient.abandon_claim

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-stove0riverhogclient-abandon-claim:31fca472f2 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-6e72067c7f"></a>
- <a id="s-fbb40de22c"></a>`distribution`: `stove0-server`
- <a id="s-bd5e7e9450"></a>`module`: `stove0_core`
- <a id="s-9b05d3c230"></a>`name`: `abandon_claim`
- <a id="s-e8ca9e9c34"></a>`owner`: `stove0_core.Stove0RiverhogClient`
- <a id="s-f7224a6d2a"></a>`unit`: `member`

### Declared structure

- <a id="s-cdb33edaef"></a>`kind`: `"method"`
- <a id="s-7cdd14dcbf"></a>`signature`: `"\"(self, record: 'WorkRecord') -> 'None'\""`

## Maintained corroboration

### Related interface records

- [stove0_core.Stove0RiverhogClient](stove0-core-stove0riverhogclient.md)

## Governing policies

- <a id="pa-6b956f9244"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-server:stove0_core](../../../evidence/sources.md#src-7558b08e7f) — `reference/stove0/application/server/src/stove0_core/__init__.py`

### Machine authority

- `/external_contract/python/stove0_core.Stove0RiverhogClient.abandon_claim`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: e33332626ff6434b6e8bc66350a43bf43342cbafb2e65b2db576cc6950ce9758 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, record: 'WorkRecord') -> 'None'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "abandon_claim",
  "owner": "stove0_core.Stove0RiverhogClient",
  "unit": "member"
}
```
