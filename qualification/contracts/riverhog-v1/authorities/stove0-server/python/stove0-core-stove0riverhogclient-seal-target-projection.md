# stove0_core.Stove0RiverhogClient.seal_target_projection

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-stove0riverhogclient-seal-tar-de8d90cdff:4dac162c4c -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-324fc88ad1"></a>
- <a id="s-9265dcee91"></a>`distribution`: `stove0-server`
- <a id="s-54f957ba81"></a>`module`: `stove0_core`
- <a id="s-078afe3699"></a>`name`: `seal_target_projection`
- <a id="s-f5f3d83d10"></a>`owner`: `stove0_core.Stove0RiverhogClient`
- <a id="s-fd0afe5575"></a>`unit`: `member`

### Declared structure

- <a id="s-986a83a5a8"></a>`kind`: `"method"`
- <a id="s-b31a0c3183"></a>`signature`: `"\"(self, record: 'WorkRecord') -> 'ArtifactDispositionSetIdentity \| None'\""`

## Maintained corroboration

### Related interface records

- [stove0_core.Stove0RiverhogClient](stove0-core-stove0riverhogclient.md)

## Governing policies

- <a id="pa-87ac0a57d1"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-server:stove0_core](../../../evidence/sources.md#src-7558b08e7f) — `reference/stove0/application/server/src/stove0_core/__init__.py`

### Machine authority

- `/external_contract/python/stove0_core.Stove0RiverhogClient.seal_target_projection`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 671571f1b5396575beef770a5e653425a3b7fdb7888758acf3c8c78cb384fbbf -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, record: 'WorkRecord') -> 'ArtifactDispositionSetIdentity | None'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "seal_target_projection",
  "owner": "stove0_core.Stove0RiverhogClient",
  "unit": "member"
}
```
