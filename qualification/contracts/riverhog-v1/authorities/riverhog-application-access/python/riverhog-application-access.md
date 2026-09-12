# riverhog_application_access

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-application-access:riverhog-application-access:4cfffe4208 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-application-access](../index.md) |
| Interface | [python](index.md) |
| Family | [modules](index.md#f-33ca04915b8c) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-495fe6be8d03"></a>
| Field | Shape |
|---|---|
| <a id="s-757b63970b1d"></a>`distribution` | "riverhog-application-access" |
| <a id="s-0ba0db78855b"></a>`exports` | additional keys=`ALL_PERMISSIONS`, `ALL_RESOURCES`, `APPLICATION_KEY_ID_PATTERN`, `APPLICATION_NAME_PATTERN`, `APPLICATION_PERMISSIONS`, `ARCHIVES_MANAGE`, `ARCHIVES_READ`, `ApplicationAccess`, `ApplicationAccessError`, `ApplicationAccessGrant`, `ApplicationAccessGrantSet`, `ApplicationKeyId`, `ApplicationName`, `ApplicationPermission`, `ApplicationResource`, `CATALOG_READ`, `COLLECTIONS_CREATE`, `COLLECTIONS_DELETE`, `COLLECTION_DESCRIPTIONS_MANAGE`, `COLLECTION_PREFIX`, `COLLECTION_SCOPED_PERMISSIONS`, `COLLECTION_TAGS_MANAGE`, `COLLECTION_TRANSFORMS_CONTROL`, `COLLECTION_TRANSFORMS_EXECUTE`, `EVENTS_READ`, `EVENTS_READ_ALL`, `KEYS_MANAGE`, `MonthlyDownloadQuotaBytes`, `PROVENANCE_EXPORT`, `PROVENANCE_READ`, `QUOTAS_MANAGE`, `RETRIEVAL_MANAGE`, `TAG_PREFIX`, `access_covers`, `collection_resource`, `normalize_access`, `permission_covers`, `permission_resources`, `resource_covers`, `tag_resource`, `validate_application_key_id`, `validate_application_name`, `validate_application_resource`, `validate_monthly_download_quota_bytes` |
| <a id="s-bbebd6deb089"></a>`module` | "riverhog_application_access" |

## Governing policies

- <a id="pa-78d7bf453460"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba506)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3eb7)
- [make build](../../../evidence/sources.md#q-d1121e35fa7a)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-application-access](../../../evidence/sources.md#src-8a8a1adad73e) — `packages/riverhog-application-access/src/riverhog_application_access/__init__.py::<module>`

### Machine authority

- `/external_contract/python/3`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 09a41f6eae0338b377ad4a80b56a4b00ee98c104c843b3ef974fd8ac6b07e309 -->

```json
{
  "distribution": "riverhog-application-access",
  "exports": {
    "ALL_PERMISSIONS": {
      "kind": "constant",
      "value": "*"
    },
    "ALL_RESOURCES": {
      "kind": "constant",
      "value": "*"
    },
    "APPLICATION_KEY_ID_PATTERN": {
      "kind": "constant",
      "value": "^[0-9a-f]{16}$"
    },
    "APPLICATION_NAME_PATTERN": {
      "kind": "constant",
      "value": "^[a-z0-9]+(?:-[a-z0-9]+)*$"
    },
    "APPLICATION_PERMISSIONS": {
      "kind": "constant",
      "value": [
        "archives:manage",
        "archives:read",
        "catalog:read",
        "collection-descriptions:manage",
        "collection-tags:manage",
        "collection-transforms:control",
        "collection-transforms:execute",
        "collections:create",
        "collections:delete",
        "events:read",
        "events:read_all",
        "keys:manage",
        "provenance:export",
        "provenance:read",
        "quotas:manage",
        "retrieval:manage"
      ]
    },
    "ARCHIVES_MANAGE": {
      "kind": "constant",
      "value": "archives:manage"
    },
    "ARCHIVES_READ": {
      "kind": "constant",
      "value": "archives:read"
    },
    "ApplicationAccess": {
      "fields": [
        {
          "default": "required",
          "name": "permission",
          "type": "'ApplicationPermission'"
        },
        {
          "default": "'*'",
          "name": "resource",
          "type": "'ApplicationResource'"
        }
      ],
      "kind": "class",
      "signature": "(permission: 'ApplicationPermission', resource: 'ApplicationResource' = '*') -> None"
    },
    "ApplicationAccessError": {
      "kind": "class",
      "signature": "unavailable"
    },
    "ApplicationAccessGrant": {
      "kind": "class",
      "members": {
        "as_access": {
          "kind": "method",
          "signature": "(self) -> 'ApplicationAccess'"
        },
        "validate_relationship": {
          "kind": "method",
          "signature": "(self) -> 'ApplicationAccessGrant'"
        }
      },
      "schema_sha256": "a587a8ba6e6662132a0d755eaea10095b5ab546cf17c3e2bb525d93630fb90e1",
      "signature": "(*, permission: ApplicationPermission, resource: ApplicationResource = '*') -> None"
    },
    "ApplicationAccessGrantSet": {
      "kind": "class",
      "members": {
        "validate_set": {
          "kind": "method",
          "signature": "(self) -> 'ApplicationAccessGrantSet'"
        }
      },
      "schema_sha256": "d10037ab0ccafe138959e093e3b62aeac69ae2b01271ebeabc7550a9ecc98897",
      "signature": "(root: 'RootModelRootType' = PydanticUndefined) -> None"
    },
    "ApplicationKeyId": {
      "kind": "type-alias",
      "value": "typing.Annotated[str, FieldInfo(annotation=NoneType, required=True, metadata=[_PydanticGeneralMetadata(pattern='^[0-9a-f]{16}$')]), AfterValidator(func=<function validate_application_key_id>)]"
    },
    "ApplicationName": {
      "kind": "type-alias",
      "value": "typing.Annotated[str, FieldInfo(annotation=NoneType, required=True, metadata=[_PydanticGeneralMetadata(pattern='^[a-z0-9]+(?:-[a-z0-9]+)*$')]), AfterValidator(func=<function validate_application_name>)]"
    },
    "ApplicationPermission": {
      "kind": "type-alias",
      "value": "typing.Literal['*', 'catalog:read', 'retrieval:manage', 'collections:create', 'collection-descriptions:manage', 'collection-transforms:control', 'collection-transforms:execute', 'collection-tags:manage', 'collections:delete', 'archives:read', 'archives:manage', 'keys:manage', 'quotas:manage', 'events:read', 'events:read_all', 'provenance:read', 'provenance:export']"
    },
    "ApplicationResource": {
      "kind": "type-alias",
      "value": "typing.Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^(?:\\\\*|tag:.+|collection:[1-9][0-9]*)$', ascii_only=None), AfterValidator(func=<function validate_application_resource>)]"
    },
    "CATALOG_READ": {
      "kind": "constant",
      "value": "catalog:read"
    },
    "COLLECTIONS_CREATE": {
      "kind": "constant",
      "value": "collections:create"
    },
    "COLLECTIONS_DELETE": {
      "kind": "constant",
      "value": "collections:delete"
    },
    "COLLECTION_DESCRIPTIONS_MANAGE": {
      "kind": "constant",
      "value": "collection-descriptions:manage"
    },
    "COLLECTION_PREFIX": {
      "kind": "constant",
      "value": "collection:"
    },
    "COLLECTION_SCOPED_PERMISSIONS": {
      "kind": "constant",
      "value": [
        "archives:manage",
        "archives:read",
        "catalog:read",
        "collection-descriptions:manage",
        "collection-tags:manage",
        "collections:delete",
        "provenance:export",
        "provenance:read",
        "retrieval:manage"
      ]
    },
    "COLLECTION_TAGS_MANAGE": {
      "kind": "constant",
      "value": "collection-tags:manage"
    },
    "COLLECTION_TRANSFORMS_CONTROL": {
      "kind": "constant",
      "value": "collection-transforms:control"
    },
    "COLLECTION_TRANSFORMS_EXECUTE": {
      "kind": "constant",
      "value": "collection-transforms:execute"
    },
    "EVENTS_READ": {
      "kind": "constant",
      "value": "events:read"
    },
    "EVENTS_READ_ALL": {
      "kind": "constant",
      "value": "events:read_all"
    },
    "KEYS_MANAGE": {
      "kind": "constant",
      "value": "keys:manage"
    },
    "MonthlyDownloadQuotaBytes": {
      "kind": "type-alias",
      "value": "typing.Annotated[int, BeforeValidator(func=<function validate_monthly_download_quota_bytes>, json_schema_input_type=PydanticUndefined), FieldInfo(annotation=NoneType, required=True, metadata=[Ge(ge=0)])]"
    },
    "PROVENANCE_EXPORT": {
      "kind": "constant",
      "value": "provenance:export"
    },
    "PROVENANCE_READ": {
      "kind": "constant",
      "value": "provenance:read"
    },
    "QUOTAS_MANAGE": {
      "kind": "constant",
      "value": "quotas:manage"
    },
    "RETRIEVAL_MANAGE": {
      "kind": "constant",
      "value": "retrieval:manage"
    },
    "TAG_PREFIX": {
      "kind": "constant",
      "value": "tag:"
    },
    "access_covers": {
      "kind": "function",
      "signature": "(grantor: 'ApplicationAccess', requested: 'ApplicationAccess') -> 'bool'"
    },
    "collection_resource": {
      "kind": "function",
      "signature": "(collection_id: 'int | str') -> 'str'"
    },
    "normalize_access": {
      "kind": "function",
      "signature": "(values: 'Iterable[ApplicationAccess | tuple[str, str]]') -> 'tuple[ApplicationAccess, ...]'"
    },
    "permission_covers": {
      "kind": "function",
      "signature": "(grantor: 'str', requested: 'str') -> 'bool'"
    },
    "permission_resources": {
      "kind": "function",
      "signature": "(access: 'Iterable[ApplicationAccess]', permission: 'str') -> 'set[str]'"
    },
    "resource_covers": {
      "kind": "function",
      "signature": "(grantor: 'str', requested: 'str') -> 'bool'"
    },
    "tag_resource": {
      "kind": "function",
      "signature": "(tag: 'str') -> 'str'"
    },
    "validate_application_key_id": {
      "kind": "function",
      "signature": "(value: 'str') -> 'str'"
    },
    "validate_application_name": {
      "kind": "function",
      "signature": "(value: 'str') -> 'str'"
    },
    "validate_application_resource": {
      "kind": "function",
      "signature": "(value: 'str') -> 'str'"
    },
    "validate_monthly_download_quota_bytes": {
      "kind": "function",
      "signature": "(value: 'object') -> 'int'"
    }
  },
  "module": "riverhog_application_access"
}
```
