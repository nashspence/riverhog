# stove0_core.Stove0RiverhogClient.release_claim

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-stove0riverhogclient-release-claim:0a562ba84b -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-f716dbb48d"></a>
- <a id="s-0670fd1a4d"></a>`distribution`: `stove0-server`
- <a id="s-97d8c60b91"></a>`module`: `stove0_core`
- <a id="s-85655e55b8"></a>`name`: `release_claim`
- <a id="s-892a81442a"></a>`owner`: `stove0_core.Stove0RiverhogClient`
- <a id="s-ed8d715c59"></a>`unit`: `member`

### Declared structure

- <a id="s-7aa9281e65"></a>`kind`: `"method"`
- <a id="s-6ba782d180"></a>`signature`: `"\"(self, record: 'WorkRecord') -> 'None'\""`

## Maintained corroboration

### Related interface records

- [stove0_core.Stove0RiverhogClient](stove0-core-stove0riverhogclient.md)

## Governing policies

- <a id="pa-24f9f5cc54"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-server:stove0_core](../../../evidence/sources.md#src-7558b08e7f) — `reference/stove0/application/server/src/stove0_core/__init__.py`

### Machine authority

- `/external_contract/python/stove0_core.Stove0RiverhogClient.release_claim`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: ae98473ea48cc895fa71648a9dadd6fa1e8a6a71c3f920902477d37cbaaf7bde -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, record: 'WorkRecord') -> 'None'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "release_claim",
  "owner": "stove0_core.Stove0RiverhogClient",
  "unit": "member"
}
```
