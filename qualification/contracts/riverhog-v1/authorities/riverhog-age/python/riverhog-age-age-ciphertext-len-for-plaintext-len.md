# riverhog_age.age_ciphertext_len_for_plaintext_len

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-age:riverhog-age-age-ciphertext-len-for-plaintext-len:62049aacc0 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-age](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-66cfdd883f"></a>
| Field | Shape |
|---|---|
| <a id="s-89f8a1e72e"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-008af04d13"></a>`distribution` | "riverhog-age" |
| <a id="s-70b5ca5fab"></a>`module` | "riverhog_age" |
| <a id="s-374f3b070a"></a>`name` | "age_ciphertext_len_for_plaintext_len" |
| <a id="s-efd9541bc4"></a>`unit` | "export" |

## Governing policies

- <a id="pa-c5c4b90fe2"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-age:riverhog_age](../../../evidence/sources.md#src-a842e50b8b) — `packages/riverhog-age/src/riverhog_age/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_age.age_ciphertext_len_for_plaintext_len`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: e2994532efa28184eb6abd3d000de88950a48ade907f24a75f55b448d649b672 -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(plaintext_size: 'int', *, age_prefix_len: 'int') -> 'int'\""
  },
  "distribution": "riverhog-age",
  "module": "riverhog_age",
  "name": "age_ciphertext_len_for_plaintext_len",
  "unit": "export"
}
```
