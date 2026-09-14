# riverhog_client.RestorePolicy

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-restorepolicy:c7ed190733 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-1b4ebbf484"></a>
- <a id="s-252f4efd13"></a>`distribution`: `riverhog-client`
- <a id="s-652b67ebde"></a>`module`: `riverhog_client`
- <a id="s-eb59bbeb3a"></a>`name`: `RestorePolicy`
- <a id="s-a51bbf45c7"></a>`unit`: `export`

### Declared structure

- <a id="s-ca006fe80c"></a>`kind`: `"type-alias"`
- <a id="s-dcc987926f"></a>`value`: `"typing.Literal['allow', 'never']"`

## Governing policies

- <a id="pa-638ef7e240"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-client:riverhog_client](../../../evidence/sources.md#src-c149020c71) — `packages/riverhog-client/src/riverhog_client/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_client.RestorePolicy`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 2bf72fc9fd294090603deda8a02438a4ab7e611a5a18707145bbea1831413343 -->

```json
{
  "contract": {
    "kind": "type-alias",
    "value": "typing.Literal['allow', 'never']"
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client",
  "name": "RestorePolicy",
  "unit": "export"
}
```
