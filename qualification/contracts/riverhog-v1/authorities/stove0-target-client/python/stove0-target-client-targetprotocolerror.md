# stove0_target_client.TargetProtocolError

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-client:stove0-target-client-targetprotocolerror:07c25f0d96 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-32eff3285b"></a>
- <a id="s-09dbe6aae0"></a>`distribution`: `stove0-target-client`
- <a id="s-e1becea158"></a>`module`: `stove0_target_client`
- <a id="s-f421c81f39"></a>`name`: `TargetProtocolError`
- <a id="s-cd92892038"></a>`unit`: `export`

### Declared structure

- <a id="s-179bb14009"></a>`kind`: `"class"`
- <a id="s-13867b4dc4"></a>`signature`: `"'(message: \\'str\\', *, failure_kind: \"Literal[\\'remote_rejection\\', \\'transport\\', \\'invalid_response\\']\", code: \\'str \| None\\' = None, observed_status: \\'int \| None\\' = None, details: \\'Mapping[str, Any] \| None\\' = None) -> \\'None\\''"`

## Governing policies

- <a id="pa-85c47fc4c3"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-target-client:stove0_target_client](../../../evidence/sources.md#src-be4c80156f) — `reference/stove0/packages/target-client/src/stove0_target_client/__init__.py`

### Machine authority

- `/external_contract/python/stove0_target_client.TargetProtocolError`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 0a466b5470047b640285a74fd73773f4f2a9ddb967399b28a286c365609dd5cb -->

```json
{
  "contract": {
    "kind": "class",
    "signature": "'(message: \\'str\\', *, failure_kind: \"Literal[\\'remote_rejection\\', \\'transport\\', \\'invalid_response\\']\", code: \\'str | None\\' = None, observed_status: \\'int | None\\' = None, details: \\'Mapping[str, Any] | None\\' = None) -> \\'None\\''"
  },
  "distribution": "stove0-target-client",
  "module": "stove0_target_client",
  "name": "TargetProtocolError",
  "unit": "export"
}
```
