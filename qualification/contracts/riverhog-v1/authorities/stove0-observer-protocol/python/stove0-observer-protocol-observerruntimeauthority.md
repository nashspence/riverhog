# stove0_observer_protocol.ObserverRuntimeAuthority

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-observer-protocol:stove0-observer-protocol-observerruntimeauthority:8689031ba5 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-observer-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-70b1c3ab83"></a>
| Field | Shape |
|---|---|
| <a id="s-00f1a9897c"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-587c69aeaf"></a>`distribution` | "stove0-observer-protocol" |
| <a id="s-058c5aa9e6"></a>`module` | "stove0_observer_protocol" |
| <a id="s-d4dd2596f3"></a>`name` | "ObserverRuntimeAuthority" |
| <a id="s-b29503ad59"></a>`unit` | "export" |

## Governing policies

- <a id="pa-8aea2907d7"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-observer-protocol:stove0_observer_protocol](../../../evidence/sources.md#src-62450e0156) — `reference/stove0/packages/observer-protocol/src/stove0_observer_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_observer_protocol.ObserverRuntimeAuthority`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 4c6249a2aed43753322f58d1b9fec06ed6c57dced5eebfaceb688047854aa29b -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "a99782500110e8937b85f3ce1843790ca1e5b7febec3c08000c176c5452cc51c",
    "signature": "\"(*, transport: Literal['riverhog-capability/v1'] = 'riverhog-capability/v1', riverhog_base_url: Annotated[str, MinLen(min_length=1), MaxLen(max_length=2048)], capability_token: Annotated[str, MinLen(min_length=1), MaxLen(max_length=4096)], allow_insecure_http: bool = False, workspace_assurance: Literal['encrypted', 'ephemeral']) -> None\""
  },
  "distribution": "stove0-observer-protocol",
  "module": "stove0_observer_protocol",
  "name": "ObserverRuntimeAuthority",
  "unit": "export"
}
```
