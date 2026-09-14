# riverhog_storage_adapter_support.StorageAdapterConformanceResult

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-storage-adapter-support:riverhog-storage-adapter-support-storagea-c664c77473:3d01d04996 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-18facb70da"></a>
- <a id="s-ffedee9ffc"></a>`distribution`: `riverhog-storage-adapter-support`
- <a id="s-384c5d2673"></a>`module`: `riverhog_storage_adapter_support`
- <a id="s-d0a87d23b2"></a>`name`: `StorageAdapterConformanceResult`
- <a id="s-d65496649d"></a>`unit`: `export`

### Declared structure

- <a id="s-f7179b75ef"></a>`kind`: `"class"`
- <a id="s-4fdefd56d9"></a>`signature`: `"\"(*, format: Literal['riverhog-storage-adapter-conformance-result/v1'] = 'riverhog-storage-adapter-conformance-result/v1', protocol: Literal['riverhog-storage-adapter/v1'] = 'riverhog-storage-adapter/v1', status: Literal['conformant'] = 'conformant', coverage: Literal['complete'] = 'complete', descriptor: riverhog_storage_adapter_protocol.protocol.AdapterDescriptor, checks: tuple[str, ...]) -> None\""`

#### Validated model schema

<a id="s-7e447d5c6b"></a>
- <a id="s-fcac4589b9"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-f58fa7e12c"></a>`checks` | yes | type="array"; items=(type="string") |  |
| <a id="s-463f28552f"></a>`coverage` | no | type="string"; const="complete" |  |
| <a id="s-96e93d011c"></a>`descriptor` | yes | #/$defs/AdapterDescriptor |  |
| <a id="s-1d41921394"></a>`format` | no | type="string"; const="riverhog-storage-adapter-conformance-result/v1" |  |
| <a id="s-85ec9d5d4e"></a>`protocol` | no | type="string"; const="riverhog-storage-adapter/v1" |  |
| <a id="s-7aadf18176"></a>`status` | no | type="string"; const="conformant" |  |

### Definitions

| Definition | Shape |
|---|---|
| <a id="s-4029f2ce9e"></a>`AdapterDescriptor` | type="object"; fields=`implementation_id`, `implementation_version`, `maximum_segment_bytes`, `maximum_segment_count`, `minimum_nonfinal_segment_bytes`, `protocol`, `read_mode`; additional keys=`additionalProperties`, `required` |

## Maintained corroboration

### Related interface records

- [riverhog_storage_adapter_support.StorageAdapterConformanceResult.validate_exact_coverage](riverhog-storage-adapter-support-storageadapterconformanceresult-validate-exact-coverage.md)

## Governing policies

- <a id="pa-840d814743"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-storage-adapter-support:riverhog_storage_adapter_support](../../../evidence/sources.md#src-284271cd54) — `packages/riverhog-storage-adapter-support/src/riverhog_storage_adapter_support/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_storage_adapter_support.StorageAdapterConformanceResult`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 0ef963706ebfa15ea243c44de0f4710eb73283c24d9fe51ca96df7649537a1d5 -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
      "$defs": {
        "AdapterDescriptor": {
          "additionalProperties": false,
          "properties": {
            "implementation_id": {
              "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
              "type": "string"
            },
            "implementation_version": {
              "maxLength": 120,
              "minLength": 1,
              "type": "string"
            },
            "maximum_segment_bytes": {
              "anyOf": [
                {
                  "minimum": 1,
                  "type": "integer"
                },
                {
                  "type": "null"
                }
              ],
              "default": null
            },
            "maximum_segment_count": {
              "anyOf": [
                {
                  "minimum": 1,
                  "type": "integer"
                },
                {
                  "type": "null"
                }
              ],
              "default": null
            },
            "minimum_nonfinal_segment_bytes": {
              "minimum": 1,
              "type": "integer"
            },
            "protocol": {
              "const": "riverhog-storage-adapter/v1",
              "default": "riverhog-storage-adapter/v1",
              "type": "string"
            },
            "read_mode": {
              "enum": [
                "immediate",
                "restore_required"
              ],
              "type": "string"
            }
          },
          "required": [
            "implementation_id",
            "implementation_version",
            "read_mode",
            "minimum_nonfinal_segment_bytes"
          ],
          "type": "object"
        }
      },
      "additionalProperties": false,
      "properties": {
        "checks": {
          "items": {
            "type": "string"
          },
          "type": "array"
        },
        "coverage": {
          "const": "complete",
          "default": "complete",
          "type": "string"
        },
        "descriptor": {
          "$ref": "#/$defs/AdapterDescriptor"
        },
        "format": {
          "const": "riverhog-storage-adapter-conformance-result/v1",
          "default": "riverhog-storage-adapter-conformance-result/v1",
          "type": "string"
        },
        "protocol": {
          "const": "riverhog-storage-adapter/v1",
          "default": "riverhog-storage-adapter/v1",
          "type": "string"
        },
        "status": {
          "const": "conformant",
          "default": "conformant",
          "type": "string"
        }
      },
      "required": [
        "descriptor",
        "checks"
      ],
      "type": "object"
    },
    "signature": "\"(*, format: Literal['riverhog-storage-adapter-conformance-result/v1'] = 'riverhog-storage-adapter-conformance-result/v1', protocol: Literal['riverhog-storage-adapter/v1'] = 'riverhog-storage-adapter/v1', status: Literal['conformant'] = 'conformant', coverage: Literal['complete'] = 'complete', descriptor: riverhog_storage_adapter_protocol.protocol.AdapterDescriptor, checks: tuple[str, ...]) -> None\""
  },
  "distribution": "riverhog-storage-adapter-support",
  "module": "riverhog_storage_adapter_support",
  "name": "StorageAdapterConformanceResult",
  "unit": "export"
}
```
