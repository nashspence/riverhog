# schemas: AppKeyOut

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-appkeyout:41aeadc2ae -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [http](index.md) |
| Family | [schemas](families/schemas/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-30b79f4da3"></a>
- <a id="s-b5bbd41ed4"></a>`title`: AppKeyOut
- <a id="s-78e9e2b54f"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-de20ad219d"></a>`access` | yes | #/components/schemas/ApplicationAccessGrantSet |  |
| <a id="s-f98913f6b5"></a>`app` | yes | #/components/schemas/ApplicationName |  |
| <a id="s-a7a363cae1"></a>`created_at` | yes | type="string" |  |
| <a id="s-f354ddeeb2"></a>`expires_at` | yes | anyOf=type="string" \| type="null" |  |
| <a id="s-f8ace5c134"></a>`id` | yes | #/components/schemas/ApplicationKeyId |  |
| <a id="s-ff83a181db"></a>`last_used_at` | yes | anyOf=type="string" \| type="null" |  |
| <a id="s-3607996617"></a>`monthly_download_quota_bytes` | yes | anyOf=#/components/schemas/MonthlyDownloadQuotaBytes \| type="null" |  |
| <a id="s-7354f9aecf"></a>`revoked_at` | yes | anyOf=type="string" \| type="null" |  |
| <a id="s-d6933d5f36"></a>`status` | yes | type="string"; enum=["active","expired","revoked"] |  |

## Maintained corroboration

### Referenced contract dossiers

- [schemas: ApplicationAccessGrantSet](schemas-applicationaccessgrantset.md)
- [schemas: ApplicationKeyId](schemas-applicationkeyid.md)
- [schemas: ApplicationName](schemas-applicationname.md)
- [schemas: MonthlyDownloadQuotaBytes](schemas-monthlydownloadquotabytes.md)

## Governing policies

- <a id="pa-b90940f90c"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc9) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/AppKeyOut`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 0318168b6bc01c609d75f00f05a851683ea524ee0344693cf434035c641b66cd -->

```json
{
  "additionalProperties": false,
  "allOf": [
    {
      "else": {
        "properties": {
          "revoked_at": {
            "type": "null"
          }
        }
      },
      "if": {
        "properties": {
          "status": {
            "const": "revoked"
          }
        }
      },
      "then": {
        "properties": {
          "revoked_at": {
            "type": "string"
          }
        }
      }
    },
    {
      "if": {
        "properties": {
          "status": {
            "const": "expired"
          }
        }
      },
      "then": {
        "properties": {
          "expires_at": {
            "type": "string"
          }
        }
      }
    }
  ],
  "properties": {
    "access": {
      "$ref": "#/components/schemas/ApplicationAccessGrantSet"
    },
    "app": {
      "$ref": "#/components/schemas/ApplicationName"
    },
    "created_at": {
      "title": "Created At",
      "type": "string"
    },
    "expires_at": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "title": "Expires At"
    },
    "id": {
      "$ref": "#/components/schemas/ApplicationKeyId"
    },
    "last_used_at": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "title": "Last Used At"
    },
    "monthly_download_quota_bytes": {
      "anyOf": [
        {
          "$ref": "#/components/schemas/MonthlyDownloadQuotaBytes"
        },
        {
          "type": "null"
        }
      ]
    },
    "revoked_at": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "title": "Revoked At"
    },
    "status": {
      "enum": [
        "active",
        "expired",
        "revoked"
      ],
      "title": "Status",
      "type": "string"
    }
  },
  "required": [
    "id",
    "app",
    "access",
    "monthly_download_quota_bytes",
    "status",
    "created_at",
    "expires_at",
    "revoked_at",
    "last_used_at"
  ],
  "title": "AppKeyOut",
  "type": "object"
}
```
