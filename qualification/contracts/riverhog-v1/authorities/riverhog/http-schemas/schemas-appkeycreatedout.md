# schemas: AppKeyCreatedOut

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:riverhog:schemas-appkeycreatedout:7fcc88a6a1 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-cd113a1d81"></a>

- <a id="s-3b25d9574e"></a>`type`: `"object"`
- <a id="s-d864794007"></a>`additionalProperties`: `false`
- <a id="s-ffc8b29a5b"></a>`required`: `["id","app","access","monthly_download_quota_bytes","status","created_at","expires_at","revoked_at","last_used_at","token"]`
- <a id="s-d5abee0256"></a>`title`: `"AppKeyCreatedOut"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-586f65365e"></a>`access` | yes | [ApplicationAccessGrantSet](schemas-applicationaccessgrantset.md) |  |
| <a id="s-48bcb68302"></a>`app` | yes | [ApplicationName](schemas-applicationname.md) |  |
| <a id="s-7b641a4bb0"></a>`created_at` | yes | type="string"; title="Created At" |  |
| <a id="s-d9ca89d4ca"></a>`expires_at` | yes | anyOf=[(type="string"); (type="null")]; title="Expires At" |  |
| <a id="s-5ccd64df56"></a>`id` | yes | [ApplicationKeyId](schemas-applicationkeyid.md) |  |
| <a id="s-9dbd92c3f8"></a>`last_used_at` | yes | anyOf=[(type="string"); (type="null")]; title="Last Used At" |  |
| <a id="s-54bec0ed94"></a>`monthly_download_quota_bytes` | yes | anyOf=[([MonthlyDownloadQuotaBytes](schemas-monthlydownloadquotabytes.md)); (type="null")] |  |
| <a id="s-e253c7e9c1"></a>`revoked_at` | yes | anyOf=[(type="string"); (type="null")]; title="Revoked At" |  |
| <a id="s-41bdd094b2"></a>`status` | yes | type="string"; enum=["active","expired","revoked"]; title="Status" |  |
| <a id="s-7def1d8177"></a>`token` | yes | type="string"; title="Token" |  |

### All must match (`allOf`)

| Rule | If schema matches | Then must match | Otherwise must match |
|---|---|---|---|
| <a id="s-10d16a3d18"></a>1 | properties={status: (const="revoked")} | properties={revoked_at: (type="string")} | properties={revoked_at: (type="null")} |
| <a id="s-f7ba579e11"></a>2 | properties={status: (const="expired")} | properties={expires_at: (type="string")} | no additional constraint |

## Maintained corroboration

### Referenced contract dossiers

- [ApplicationAccessGrantSet](schemas-applicationaccessgrantset.md)
- [ApplicationKeyId](schemas-applicationkeyid.md)
- [ApplicationName](schemas-applicationname.md)
- [MonthlyDownloadQuotaBytes](schemas-monthlydownloadquotabytes.md)

## Governing policies

- <a id="pa-04b352a05c"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc9) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L333)

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/AppKeyCreatedOut`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: b48fb472f2850c9929f5c7614e7848aef64643b02496b07bd0661060b148c76b -->

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
    },
    "token": {
      "title": "Token",
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
    "last_used_at",
    "token"
  ],
  "title": "AppKeyCreatedOut",
  "type": "object"
}
```

</details>
