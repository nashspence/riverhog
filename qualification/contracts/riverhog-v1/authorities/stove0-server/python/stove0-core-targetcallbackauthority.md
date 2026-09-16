# stove0_core.TargetCallbackAuthority

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-targetcallbackauthority:a68df26b9f -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-51e0a1cb34"></a>
- <a id="s-ede5e032d7"></a>`distribution`: `stove0-server`
- <a id="s-0b87a69cfd"></a>`module`: `stove0_core`
- <a id="s-c2c117030b"></a>`name`: `TargetCallbackAuthority`
- <a id="s-49ce5b6c5d"></a>`unit`: `export`

### Declared structure

- <a id="s-7cdfcbabdd"></a>`kind`: `"class"`
- <a id="s-2dac713eb9"></a>`signature`: `"\"(store: 'WorkStore', *, signing_key: 'str', base_url: 'str', allow_insecure_http: 'bool', ttl_seconds: 'int', operations: 'OperationAuthority \| None' = None, projector: 'ProductionProjector \| None' = None, seal_batch_size: 'int' = 100) -> 'None'\""`

## Maintained corroboration

### Related interface records

- [declare_disposition](stove0-core-targetcallbackauthority-declare-disposition.md)
- [declare_source_edge](stove0-core-targetcallbackauthority-declare-source-edge.md)
- [declare_output](stove0-core-targetcallbackauthority-declare-output.md)
- [input_page](stove0-core-targetcallbackauthority-input-page.md)
- [issue_access](stove0-core-targetcallbackauthority-issue-access.md)
- [process_due_production_seals](stove0-core-targetcallbackauthority-process-due-production-seals.md)
- [seal_production](stove0-core-targetcallbackauthority-seal-production.md)

## Governing policies

- <a id="pa-85ccff8e40"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-server:stove0_core](../../../evidence/sources.md#src-7558b08e7f) — `reference/stove0/application/server/src/stove0_core/__init__.py`

### Machine authority

- `/external_contract/python/stove0_core.TargetCallbackAuthority`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: e5b8dd3e8d03ad8781eaa64dd140fcc2cd3b7f8eb055e1b2f46e807ef728e8e6 -->

```json
{
  "contract": {
    "kind": "class",
    "signature": "\"(store: 'WorkStore', *, signing_key: 'str', base_url: 'str', allow_insecure_http: 'bool', ttl_seconds: 'int', operations: 'OperationAuthority | None' = None, projector: 'ProductionProjector | None' = None, seal_batch_size: 'int' = 100) -> 'None'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "TargetCallbackAuthority",
  "unit": "export"
}
```

</details>
