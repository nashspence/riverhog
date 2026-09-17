# stove0_core.TargetCallbackAuthority.declare_source_edge

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-targetcallbackauthority-decla-ee98f072ad:1084394ccd -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-6d6f263297"></a>
- <a id="s-7030e75c4c"></a>`distribution`: `stove0-server`
- <a id="s-b8d7b0a3bc"></a>`module`: `stove0_core`
- <a id="s-2946fae855"></a>`name`: `declare_source_edge`
- <a id="s-2d251c5da7"></a>`owner`: `stove0_core.TargetCallbackAuthority`
- <a id="s-d683cedf11"></a>`unit`: `member`

### Declared structure

- <a id="s-f284ab4c14"></a>`kind`: `"method"`
- <a id="s-33963184fd"></a>`signature`: `"\"(self, token: 'str', *, job_id: 'str', edge: 'OutputSourceEdge') -> 'None'\""`

## Maintained corroboration

### Related interface records

- [TargetCallbackAuthority](stove0-core-targetcallbackauthority.md)

## Governing policies

- <a id="pa-7f19d1cdee"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-server:stove0_core](../../../evidence/sources.md#src-7558b08e7f) — [reference/stove0/application/server/src/stove0\_core/\_\_init\_\_.py](../../../../../../reference/stove0/application/server/src/stove0_core/__init__.py)

### Machine authority

- `/external_contract/python/stove0_core.TargetCallbackAuthority.declare_source_edge`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: a05dfb3e5eeaf71a51b32874cc2a5925debf5045aa18d3c75c8aa0f1480da23e -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, token: 'str', *, job_id: 'str', edge: 'OutputSourceEdge') -> 'None'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "declare_source_edge",
  "owner": "stove0_core.TargetCallbackAuthority",
  "unit": "member"
}
```

</details>
