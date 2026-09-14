# stove0_core.RiverhogControlPort.release_claim

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-riverhogcontrolport-release-claim:a498ad98c5 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-1fd09437ee"></a>
- <a id="s-841cf4becd"></a>`distribution`: `stove0-server`
- <a id="s-c56e6649d0"></a>`module`: `stove0_core`
- <a id="s-b4afa86f44"></a>`name`: `release_claim`
- <a id="s-5cbaf65d3c"></a>`owner`: `stove0_core.RiverhogControlPort`
- <a id="s-db21328db4"></a>`unit`: `member`

### Declared structure

- <a id="s-4a3132301b"></a>`kind`: `"method"`
- <a id="s-f9a898fe7c"></a>`signature`: `"\"(self, record: 'WorkRecord') -> 'None'\""`

## Maintained corroboration

### Related interface records

- [stove0_core.RiverhogControlPort](stove0-core-riverhogcontrolport.md)

## Governing policies

- <a id="pa-adbd123910"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-server:stove0_core](../../../evidence/sources.md#src-7558b08e7f) — `reference/stove0/application/server/src/stove0_core/__init__.py`

### Machine authority

- `/external_contract/python/stove0_core.RiverhogControlPort.release_claim`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: ce620b9e0b91d4ac7880b448b013485b350149d99f76420940cd56e0a21cb9a5 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, record: 'WorkRecord') -> 'None'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "release_claim",
  "owner": "stove0_core.RiverhogControlPort",
  "unit": "member"
}
```
