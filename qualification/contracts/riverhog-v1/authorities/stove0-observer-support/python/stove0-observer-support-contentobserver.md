# stove0_observer_support.ContentObserver

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-observer-support:stove0-observer-support-contentobserver:4063ce8081 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-observer-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-75b2f4d2c6"></a>
| Field | Shape |
|---|---|
| <a id="s-9d034b49fa"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-4df07fd152"></a>`distribution` | "stove0-observer-support" |
| <a id="s-be03f2bfc7"></a>`module` | "stove0_observer_support" |
| <a id="s-7e40c4a2a7"></a>`name` | "ContentObserver" |
| <a id="s-d65709d844"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [stove0_observer_support.ContentObserver.descriptor](stove0-observer-support-contentobserver-descriptor.md)
- [stove0_observer_support.ContentObserver.observe](stove0-observer-support-contentobserver-observe.md)

## Governing policies

- <a id="pa-1fba9aab9e"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-observer-support:stove0_observer_support](../../../evidence/sources.md#src-13bf3acd32) — `reference/stove0/packages/observer-support/src/stove0_observer_support/__init__.py`

### Machine authority

- `/external_contract/python/stove0_observer_support.ContentObserver`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 1cc0a539e61abb849d3da00bb8284009631c1c2ec6e547e39b39d66e65e55e2f -->

```json
{
  "contract": {
    "kind": "class",
    "signature": "'(*args, **kwargs)'"
  },
  "distribution": "stove0-observer-support",
  "module": "stove0_observer_support",
  "name": "ContentObserver",
  "unit": "export"
}
```
