# riverhog_protocol.OperationIdentity.as_dict

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-operationidentity-as-dict:46431d21f3 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-be851724a7"></a>
| Field | Shape |
|---|---|
| <a id="s-0b89ff4337"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-1d4e30efe7"></a>`distribution` | "riverhog-protocol" |
| <a id="s-d6511c35cc"></a>`module` | "riverhog_protocol" |
| <a id="s-0571d99fe6"></a>`name` | "as_dict" |
| <a id="s-bacd6cdd4f"></a>`owner` | "riverhog_protocol.OperationIdentity" |
| <a id="s-c3816d7b2e"></a>`unit` | "member" |

## Maintained corroboration

### Related interface records

- [riverhog_protocol.OperationIdentity](riverhog-protocol-operationidentity.md)

## Governing policies

- <a id="pa-e557f72381"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources.md#src-19e35f15d9) — `packages/riverhog-protocol/src/riverhog_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_protocol.OperationIdentity.as_dict`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 4c63c5135f40998e05c1ca41214e7057b6f560fbf9d51fe3aaff74f803ee0ccf -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'dict[str, object]'\""
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "as_dict",
  "owner": "riverhog_protocol.OperationIdentity",
  "unit": "member"
}
```
