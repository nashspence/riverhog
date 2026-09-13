# stove0_target_support.Sha256

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-support:stove0-target-support-sha256:aaa89c578b -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-88a3d67f9c"></a>
| Field | Shape |
|---|---|
| <a id="s-d5a58995aa"></a>`contract` | type="typing._AnnotatedAlias"; additional keys=`kind` |
| <a id="s-da801a1e6c"></a>`distribution` | "stove0-target-support" |
| <a id="s-76244f6e5b"></a>`module` | "stove0_target_support" |
| <a id="s-99b41a8149"></a>`name` | "Sha256" |
| <a id="s-7526e6addb"></a>`unit` | "export" |

## Governing policies

- <a id="pa-6956e0b618"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-target-support:stove0_target_support](../../../evidence/sources.md#src-3c01163237) — `reference/stove0/packages/target-support/src/stove0_target_support/__init__.py`

### Machine authority

- `/external_contract/python/stove0_target_support.Sha256`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 08241ee7c16e30de853e58e543b7ab39c64999e324fcb5499ef84732c45a8a3b -->

```json
{
  "contract": {
    "kind": "object",
    "type": "typing._AnnotatedAlias"
  },
  "distribution": "stove0-target-support",
  "module": "stove0_target_support",
  "name": "Sha256",
  "unit": "export"
}
```
