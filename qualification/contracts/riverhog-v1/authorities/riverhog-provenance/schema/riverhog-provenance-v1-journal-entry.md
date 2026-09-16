# Riverhog provenance v1 journal entry

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: schema:riverhog-provenance:riverhog-provenance-v1-journal-entry:77c4c26df7 -->

One immutable entry in a hash-chained RFC 7464 per-file provenance journal.

| Audit field | Value |
|---|---|
| Authority | [riverhog-provenance](../index.md) |
| Interface | [Schemas](index.md) |

## External contract

<a id="s-4efcfc32f7"></a>

- <a id="s-102bcf843f"></a>`type`: `"object"`
- <a id="s-d5bbb6637a"></a>`$id`: `"https://nashspence.github.io/riverhog/v1/provenance/journal-entry.schema.json"`
- <a id="s-422c66d18d"></a>`$schema`: `"https://json-schema.org/draft/2020-12/schema"`
- <a id="s-e4df2588d6"></a>`additionalProperties`: `false`
- <a id="s-3befa3a00a"></a>`description`: `"One immutable entry in a hash-chained RFC 7464 per-file provenance journal."`
- <a id="s-6c23c28f02"></a>`required`: `["$schema","profile","schema_version","id","type","journal_id","sequence","recorded_at","recorded_by_agent_id","entry_kind","body"]`
- <a id="s-532513231b"></a>`title`: `"Riverhog provenance v1 journal entry"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-63c9dee775"></a>`$schema` | yes | const="https://nashspence.github.io/riverhog/v1/provenance/journal-entry.schema.json" |  |
| <a id="s-601285dd16"></a>`body` | yes | type="object" |  |
| <a id="s-b4bb88b15a"></a>`entry_kind` | yes | type="string"; enum=["journal_init","assertion","correction","checkpoint"] |  |
| <a id="s-ffd569e30a"></a>`id` | yes | [urnUuid](#s-b9f9301bba) |  |
| <a id="s-61ddb08d61"></a>`journal_id` | yes | [urnUuid](#s-b9f9301bba) |  |
| <a id="s-0bcb488eba"></a>`notes` | no | type="array"; items=([nonEmptyString](#s-ddcf450d4d)); minItems=1; uniqueItems=true |  |
| <a id="s-e2b6e7a7c7"></a>`previous_entry` | no | [entryReference](#s-4a73d347fd) |  |
| <a id="s-b17b7abdd8"></a>`profile` | yes | const="https://nashspence.github.io/riverhog/v1/provenance" |  |
| <a id="s-dc9eae8f13"></a>`recorded_at` | yes | [utcDateTime](#s-57f05b370e) |  |
| <a id="s-6bd89aa5b2"></a>`recorded_by_agent_id` | yes | [urnUuid](#s-b9f9301bba) |  |
| <a id="s-b6dfb2d331"></a>`recording_environment_id` | no | [urnUuid](#s-b9f9301bba) |  |
| <a id="s-93c9c7970e"></a>`schema_version` | yes | const="1.0.0" |  |
| <a id="s-a755e6074e"></a>`sequence` | yes | type="integer"; minimum=0; maximum=9223372036854775807 |  |
| <a id="s-788bc03ace"></a>`type` | yes | const="riverhog_provenance_journal_entry" |  |

### All must match (`allOf`)

| Rule | If schema matches | Then must match | Otherwise must match |
|---|---|---|---|
| <a id="s-4b6f2de7a8"></a>1 | properties={entry_kind: (const="journal_init")}; required=["entry_kind"] | not=(required=["previous_entry"]); properties={body: ([journalInitBody](#s-8b0ea68958)); sequence: (const=0)} | no additional constraint |
| <a id="s-e3af1d9566"></a>2 | properties={entry_kind: (const="assertion")}; required=["entry_kind"] | properties={body: ([assertionBody](#s-d348c1e871)); sequence: (minimum=1)}; required=["previous_entry"] | no additional constraint |
| <a id="s-a41fcbe302"></a>3 | properties={entry_kind: (const="correction")}; required=["entry_kind"] | properties={body: ([correctionBody](#s-9293f3fd5e)); sequence: (minimum=1)}; required=["previous_entry"] | no additional constraint |
| <a id="s-3373b688c7"></a>4 | properties={entry_kind: (const="checkpoint")}; required=["entry_kind"] | properties={body: ([checkpointBody](#s-edbb3f1265)); sequence: (minimum=1)}; required=["previous_entry"] | no additional constraint |

### Definitions

- [absoluteUri](#s-088a6da7a6)
- [accessMetadata](#s-f7179075a7)
- [activityTime](#s-afa0679572)
- [agent](#s-73d177dbf5)
- [assertionBody](#s-d348c1e871)
- [association](#s-bdc5771284)
- [booleanValue](#s-72c05fe31a)
- [byteString](#s-f8865efa4d)
- [bytesValue](#s-57e550bbaf)
- [captureDetail](#s-f1bbe8d25c)
- [captureEvent](#s-67bdd25fe2)
- [capturedValue](#s-14ff4aeeaf)
- [checkpointBody](#s-edbb3f1265)
- [checkpointCounts](#s-62a0a91aae)
- [comparisonDimension](#s-956f3fc8b8)
- [comparisonRelation](#s-b92aeb48b4)
- [contentDescription](#s-e5941ad180)
- [continuityBasis](#s-7cf5239b36)
- [continuityRelation](#s-37e4bccab2)
- [correctionBody](#s-9293f3fd5e)
- [coverage](#s-ec32049096)
- [coverageStatus](#s-e778f73563)
- [decimalValue](#s-24acd86cba)
- [derivationRelation](#s-1d0efc506d)
- [diagnostic](#s-5eb1052892)
- [digest](#s-1e1e5e54b0)
- [digestOnlyValue](#s-b87a395047)
- [entityReferenceValue](#s-47030d684d)
- [entryReference](#s-4a73d347fd)
- [environment](#s-20b7cca542)
- [fieldSourceDescriptor](#s-dd3ca68abf)
- [fileLineage](#s-42ed3b06f0)
- [fileState](#s-72ed1caeab)
- [filesystem](#s-92f8e87e33)
- [filesystemMetadata](#s-2c6dad9ae6)
- [generationRelation](#s-fd99a1f0e8)
- [graphFragment](#s-b3f23dc219)
- [hardwareAgent](#s-2d4ca24d8e)
- [host](#s-03a9361935)
- [identifier](#s-e6d4c3ce94)
- [integerValue](#s-e7a29598b5)
- [interpretation](#s-f201133ac4)
- [invalidationRelation](#s-7be03ea942)
- [journalInitBody](#s-8b0ea68958)
- [journalPolicy](#s-31e80eaaa2)
- [jsonValue](#s-461d5ccad6)
- [kernel](#s-2920056f4c)
- [locator](#s-334ec6328c)
- [nativeCoverageCategory](#s-cc468e7930)
- [nativeMetadata](#s-29df5b2baf)
- [nonEmptyString](#s-ddcf450d4d)
- [nonNullJson](#s-899e812405)
- [observedIdentifier](#s-325bfde37a)
- [operatingSystem](#s-e585eafb76)
- [organizationAgent](#s-a050110c08)
- [payloadBinding](#s-86c27faf36)
- [personAgent](#s-5d421881fe)
- [portableString](#s-94d0b65f81)
- [principal](#s-dfb4db485f)
- [processDetail](#s-05bd4b5b33)
- [processEvidence](#s-9ccea05d10)
- [provenanceContractReference](#s-e81f29b9b5)
- [provenanceObserverReference](#s-307ea8eb10)
- [relation](#s-a65a100c0b)
- [relationRole](#s-e8775f45df)
- [runtime](#s-a011de07f6)
- [semanticAssertion](#s-2d2165b5a6)
- [sha256Hex](#s-1ceccb1b61)
- [softwareAgent](#s-e4f49d8758)
- [sourceDescriptor](#s-6bcbe0dfe1)
- [sourcePlatform](#s-e5152341be)
- [stateReference](#s-6bc47c0dba)
- [textValue](#s-ce6e164ba2)
- [timestampObservation](#s-f1f3653cef)
- [timestampValue](#s-9d92b09f3e)
- [transitionActivity](#s-95e37398de)
- [typedValue](#s-de01b50b5c)
- [uriValue](#s-6dc8938549)
- [urnUuid](#s-b9f9301bba)
- [usageRelation](#s-2a66a85deb)
- [utcDateTime](#s-57f05b370e)

### <a id="s-088a6da7a6"></a>definition `absoluteUri`

- <a id="s-8511aa13ff"></a>`type`: `"string"`
- <a id="s-d56bc8276a"></a>`format`: `"uri"`
- <a id="s-d7128f6f93"></a>`pattern`: `"^[^\\u0000\\uD800-\\uDFFF]+$"`

### <a id="s-f7179075a7"></a>definition `accessMetadata`

- <a id="s-5ad6ac9657"></a>`type`: `"object"`
- <a id="s-6e70cc72b2"></a>`additionalProperties`: `false`
- <a id="s-815cb11670"></a>`minProperties`: `1`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-140557bb02"></a>`group` | no | [principal](#s-dfb4db485f) |  |
| <a id="s-0d0f30bc42"></a>`owner` | no | [principal](#s-dfb4db485f) |  |
| <a id="s-3d49292458"></a>`posix_mode` | no | type="string"; pattern="^[0-7]{4}$" |  |

### <a id="s-afa0679572"></a>definition `activityTime`

- <a id="s-c985ef62a5"></a>`type`: `"object"`
- <a id="s-26e4efe715"></a>`additionalProperties`: `false`
- <a id="s-4230d4e7e4"></a>`required`: `["status"]`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-aeee53005f"></a>`ended_at` | no | [utcDateTime](#s-57f05b370e) |  |
| <a id="s-006470595f"></a>`note` | no | [nonEmptyString](#s-ddcf450d4d) |  |
| <a id="s-8c0e78386b"></a>`started_at` | no | [utcDateTime](#s-57f05b370e) |  |
| <a id="s-79bae2e0bb"></a>`status` | yes | type="string"; enum=["exact","approximate","bounded","unknown"] |  |

#### All must match (`allOf`)

| Rule | If schema matches | Then must match | Otherwise must match |
|---|---|---|---|
| <a id="s-a1095ba87e"></a>1 | properties={status: (const="exact")}; required=["status"] | required=["started_at","ended_at"] | no additional constraint |
| <a id="s-32cd5f2dca"></a>2 | properties={status: (const="approximate")}; required=["status"] | anyOf=(required=["started_at"]) \| (required=["ended_at"]); required=["note"] | no additional constraint |
| <a id="s-0e1dedf71d"></a>3 | properties={status: (const="bounded")}; required=["status"] | required=["started_at","ended_at","note"] | no additional constraint |
| <a id="s-b91a118f2a"></a>4 | properties={status: (const="unknown")}; required=["status"] | not=(anyOf=(required=["started_at"]) \| (required=["ended_at"])); required=["note"] | no additional constraint |

### <a id="s-73d177dbf5"></a>definition `agent`


#### Exactly one must match (`oneOf`)

| Alternative | Schema |
|---|---|
| <a id="s-75edc525d6"></a>1 | [softwareAgent](#s-e4f49d8758) |
| <a id="s-2eb6cb235c"></a>2 | [personAgent](#s-5d421881fe) |
| <a id="s-c77f9606a3"></a>3 | [organizationAgent](#s-a050110c08) |
| <a id="s-611b536964"></a>4 | [hardwareAgent](#s-2d4ca24d8e) |

### <a id="s-d348c1e871"></a>definition `assertionBody`

- <a id="s-146beda5b9"></a>`type`: `"object"`
- <a id="s-60b5c621e3"></a>`additionalProperties`: `false`
- <a id="s-bd2a6140d2"></a>`required`: `["assertions"]`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-f058a5c280"></a>`assertions` | yes | [graphFragment](#s-b3f23dc219) |  |

### <a id="s-bdc5771284"></a>definition `association`

- <a id="s-5035777478"></a>`type`: `"object"`
- <a id="s-81b162a6ca"></a>`additionalProperties`: `false`
- <a id="s-1acb62fe69"></a>`required`: `["agent_id","role"]`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-1b04847477"></a>`agent_id` | yes | [urnUuid](#s-b9f9301bba) |  |
| <a id="s-de996dde88"></a>`plan_id` | no | [absoluteUri](#s-088a6da7a6) |  |
| <a id="s-0e7b783dfe"></a>`role` | yes | type="string"; enum=["executing_software","operator","responsible_organization","authorizing_agent","capture_device","process_owner","creator","editor","reviewer","other"] |  |
| <a id="s-37e5a58bdc"></a>`role_uri` | no | [absoluteUri](#s-088a6da7a6) |  |

#### All must match (`allOf`)

| Rule | If schema matches | Then must match | Otherwise must match |
|---|---|---|---|
| <a id="s-c381530617"></a>1 | properties={role: (const="other")}; required=["role"] | required=["role_uri"] | no additional constraint |
| <a id="s-3a72dd1a2b"></a>2 | properties={role: (const="executing_software")}; required=["role"] | required=["plan_id"] | no additional constraint |

### <a id="s-72c05fe31a"></a>definition `booleanValue`

- <a id="s-d3e48ff103"></a>`type`: `"object"`
- <a id="s-8abaf73a24"></a>`additionalProperties`: `false`
- <a id="s-10ebf16d27"></a>`required`: `["type","data"]`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-5c49f63120"></a>`data` | yes | type="boolean" |  |
| <a id="s-97db77facd"></a>`type` | yes | const="boolean" |  |

### <a id="s-f8865efa4d"></a>definition `byteString`

- <a id="s-d31ac76c92"></a>`type`: `"object"`
- <a id="s-3cb1630226"></a>`additionalProperties`: `false`
- <a id="s-c4ba9cd6c8"></a>`required`: `["encoding","data","byte_length"]`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-20d5f4d688"></a>`byte_length` | yes | type="integer"; minimum=0; maximum=9223372036854775807 |  |
| <a id="s-151598ca05"></a>`data` | yes | type="string"; pattern="^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==\|[A-Za-z0-9+/]{3}=)?$" |  |
| <a id="s-ebc2a4d88d"></a>`digests` | no | type="array"; items=([digest](#s-1e1e5e54b0)); minItems=1; uniqueItems=true |  |
| <a id="s-cf5d847cda"></a>`encoding` | yes | const="base64" |  |
| <a id="s-36e655e32f"></a>`media_type` | no | [nonEmptyString](#s-ddcf450d4d) |  |

### <a id="s-57e550bbaf"></a>definition `bytesValue`

- <a id="s-71f441a567"></a>`type`: `"object"`
- <a id="s-56b656436a"></a>`additionalProperties`: `false`
- <a id="s-751b30b8dd"></a>`required`: `["type","encoding","data","byte_length"]`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-d7fe9175c0"></a>`byte_length` | yes | type="integer"; minimum=0; maximum=9223372036854775807 |  |
| <a id="s-1e58518787"></a>`data` | yes | type="string"; pattern="^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==\|[A-Za-z0-9+/]{3}=)?$" |  |
| <a id="s-4426948ec3"></a>`digests` | no | type="array"; items=([digest](#s-1e1e5e54b0)); minItems=1; uniqueItems=true |  |
| <a id="s-68230546c4"></a>`encoding` | yes | const="base64" |  |
| <a id="s-44a7e5e77d"></a>`media_type` | no | [nonEmptyString](#s-ddcf450d4d) |  |
| <a id="s-5e30fedea3"></a>`type` | yes | const="bytes" |  |

### <a id="s-f1bbe8d25c"></a>definition `captureDetail`

- <a id="s-9463fa52ab"></a>`type`: `"object"`
- <a id="s-95feaf47b3"></a>`additionalProperties`: `false`
- <a id="s-e53a7ee5b2"></a>`minProperties`: `1`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-6352076890"></a>`command_line` | no | type="array"; items=([portableString](#s-94d0b65f81)); minItems=1 |  |
| <a id="s-cbe6eb9e18"></a>`configuration_digest` | no | [digest](#s-1e1e5e54b0) |  |
| <a id="s-023583be2a"></a>`profile_id` | no | [absoluteUri](#s-088a6da7a6) |  |
| <a id="s-61f0f53718"></a>`provenance_observer` | no | [provenanceObserverReference](#s-307ea8eb10) |  |
| <a id="s-626ad38139"></a>`working_directory` | no | [locator](#s-334ec6328c) |  |

### <a id="s-67bdd25fe2"></a>definition `captureEvent`

- <a id="s-fe3ceff9d6"></a>`type`: `"object"`
- <a id="s-bac1745a51"></a>`additionalProperties`: `false`
- <a id="s-8f4feff176"></a>`description`: `"Observation Activity/PREMIS Event that used a file state and technical environment to generate provenance assertions. It does not generate the file state."`
- <a id="s-5a6067de2c"></a>`required`: `["id","type","state_id","operations","started_at","ended_at","outcome","consistency","environment_id","associations","coverage"]`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `associations` | yes | [See definition `captureEvent` · field `associations`](#s-42651f0d7d) |  |
| <a id="s-d074fa4c13"></a>`consistency` | yes | type="string"; enum=["snapshot","verified_unchanged","best_effort","unknown"] |  |
| <a id="s-ae97c485b5"></a>`coverage` | yes | [coverage](#s-ec32049096) |  |
| <a id="s-83addb5235"></a>`detail` | no | [captureDetail](#s-f1bbe8d25c) |  |
| <a id="s-e1676a2787"></a>`diagnostics` | no | type="array"; items=([diagnostic](#s-5eb1052892)); minItems=1 |  |
| <a id="s-217e8aa59d"></a>`ended_at` | yes | [utcDateTime](#s-57f05b370e) |  |
| <a id="s-47d3a58a48"></a>`environment_id` | yes | [urnUuid](#s-b9f9301bba) |  |
| <a id="s-e9a9326bcb"></a>`id` | yes | [urnUuid](#s-b9f9301bba) |  |
| <a id="s-9026ae9f30"></a>`notes` | no | type="array"; items=([nonEmptyString](#s-ddcf450d4d)); minItems=1; uniqueItems=true |  |
| `operations` | yes | [See definition `captureEvent` · field `operations`](#s-e61e291f8a) |  |
| <a id="s-2a30f97497"></a>`outcome` | yes | type="string"; enum=["success","partial"] |  |
| <a id="s-689a728534"></a>`started_at` | yes | [utcDateTime](#s-57f05b370e) |  |
| <a id="s-d59f514069"></a>`state_id` | yes | [urnUuid](#s-b9f9301bba) |  |
| <a id="s-de89c68410"></a>`type` | yes | const="file_state_capture" |  |

#### All must match (`allOf`)

| Rule | If schema matches | Then must match | Otherwise must match |
|---|---|---|---|
| <a id="s-1eebdd129f"></a>1 | properties={outcome: (const="partial")}; required=["outcome"] | required=["diagnostics"] | no additional constraint |

### <a id="s-14ff4aeeaf"></a>definition `capturedValue`


#### Exactly one must match (`oneOf`)

| Alternative | Schema |
|---|---|
| <a id="s-e1aa1d4fda"></a>1 | [bytesValue](#s-57e550bbaf) |
| <a id="s-469dd0ae3e"></a>2 | [textValue](#s-ce6e164ba2) |
| <a id="s-4906cb826d"></a>3 | [integerValue](#s-e7a29598b5) |
| <a id="s-376feee4b3"></a>4 | [decimalValue](#s-24acd86cba) |
| <a id="s-c1410a6394"></a>5 | [booleanValue](#s-72c05fe31a) |
| <a id="s-ca0f9e057f"></a>6 | [timestampValue](#s-9d92b09f3e) |
| <a id="s-b654f2e38a"></a>7 | [uriValue](#s-6dc8938549) |
| <a id="s-621b2ea291"></a>8 | [entityReferenceValue](#s-47030d684d) |
| <a id="s-b07129eec0"></a>9 | [jsonValue](#s-461d5ccad6) |

### <a id="s-edbb3f1265"></a>definition `checkpointBody`

- <a id="s-3f0cf6a25e"></a>`type`: `"object"`
- <a id="s-db5f692066"></a>`additionalProperties`: `false`
- <a id="s-6fe675d4b0"></a>`description`: `"A cumulative checkpoint over exact RFC 7464 bytes through the preceding entry. Tail truncation is detectable only when a checkpoint or tail digest is anchored externally."`
- <a id="s-5918061935"></a>`required`: `["checkpoint_kind","covered_through","stream_prefix_sha256","counts"]`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-d8d13f8f54"></a>`checkpoint_kind` | yes | type="string"; enum=["periodic","transfer","archive_ingest","publication","other"] |  |
| <a id="s-da9dc2d09b"></a>`checkpoint_kind_uri` | no | [absoluteUri](#s-088a6da7a6) |  |
| <a id="s-3dc5d6234c"></a>`counts` | yes | [checkpointCounts](#s-62a0a91aae) |  |
| <a id="s-27c982cf0e"></a>`covered_through` | yes | [entryReference](#s-4a73d347fd) |  |
| <a id="s-9b70cd63a6"></a>`note` | no | [nonEmptyString](#s-ddcf450d4d) |  |
| <a id="s-7525485913"></a>`stream_prefix_sha256` | yes | [sha256Hex](#s-1ceccb1b61) |  |

#### All must match (`allOf`)

| Rule | If schema matches | Then must match | Otherwise must match |
|---|---|---|---|
| <a id="s-1f6ff11e8b"></a>1 | properties={checkpoint_kind: (const="other")}; required=["checkpoint_kind"] | required=["checkpoint_kind_uri"] | no additional constraint |

### <a id="s-62a0a91aae"></a>definition `checkpointCounts`

- <a id="s-b7139349e3"></a>`type`: `"object"`
- <a id="s-6672f7b89b"></a>`additionalProperties`: `false`
- <a id="s-bc376c3caa"></a>`required`: `["entries"]`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-92b955b86b"></a>`activities` | no | type="integer"; minimum=0; maximum=9223372036854775807 |  |
| <a id="s-fd70682b59"></a>`captures` | no | type="integer"; minimum=0; maximum=9223372036854775807 |  |
| <a id="s-c697031463"></a>`entries` | yes | type="integer"; minimum=1; maximum=9223372036854775807 |  |
| <a id="s-c3f33d3b72"></a>`lineages` | no | type="integer"; minimum=0; maximum=9223372036854775807 |  |
| <a id="s-e41cbc72e4"></a>`relations` | no | type="integer"; minimum=0; maximum=9223372036854775807 |  |
| <a id="s-917de38a38"></a>`states` | no | type="integer"; minimum=0; maximum=9223372036854775807 |  |

### <a id="s-956f3fc8b8"></a>definition `comparisonDimension`

- <a id="s-1f2d735aa5"></a>`type`: `"object"`
- <a id="s-6505641cd9"></a>`additionalProperties`: `false`
- <a id="s-5f81be9e09"></a>`required`: `["dimension","result","basis"]`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-f7e5cae39a"></a>`basis` | yes | type="array"; items=(type="string"; enum=["size","sha-256","exact_json_value","byte_exact","normalized_value","complete_capture","producer_assertion"]); minItems=1; uniqueItems=true |  |
| <a id="s-8f9a857b9c"></a>`dimension` | yes | type="string"; enum=["primary_content","locator","timestamps","ownership","permissions","native_identifiers","native_metadata"] |  |
| <a id="s-dbee4e8572"></a>`note` | no | [nonEmptyString](#s-ddcf450d4d) |  |
| <a id="s-22d34a41d7"></a>`result` | yes | type="string"; enum=["matching_fixity","equal","different","not_observed","not_comparable","indeterminate"] |  |

### <a id="s-b92aeb48b4"></a>definition `comparisonRelation`

- <a id="s-15ad60fdcf"></a>`type`: `"object"`
- <a id="s-c14472d302"></a>`additionalProperties`: `false`
- <a id="s-df1e379391"></a>`required`: `["id","type","from_state","to_state","from_capture_id","to_capture_id","compared_at","dimensions","asserted_by_agent_id","confidence"]`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-0e71e01354"></a>`asserted_by_agent_id` | yes | [urnUuid](#s-b9f9301bba) |  |
| <a id="s-c57ff23929"></a>`compared_at` | yes | [utcDateTime](#s-57f05b370e) |  |
| <a id="s-6e2a53bd74"></a>`confidence` | yes | type="string"; enum=["high","medium","low","unknown"] |  |
| <a id="s-089c1457ee"></a>`dimensions` | yes | type="array"; items=([comparisonDimension](#s-956f3fc8b8)); minItems=1; uniqueItems=true |  |
| <a id="s-2314ebae93"></a>`from_capture_id` | yes | [urnUuid](#s-b9f9301bba) |  |
| <a id="s-53847edd1d"></a>`from_state` | yes | [stateReference](#s-6bc47c0dba) |  |
| <a id="s-7b2c96c64f"></a>`id` | yes | [urnUuid](#s-b9f9301bba) |  |
| <a id="s-7af1f6b6a5"></a>`notes` | no | type="array"; items=([nonEmptyString](#s-ddcf450d4d)); minItems=1; uniqueItems=true |  |
| <a id="s-03e5513f01"></a>`to_capture_id` | yes | [urnUuid](#s-b9f9301bba) |  |
| <a id="s-f17f9b5bbb"></a>`to_state` | yes | [stateReference](#s-6bc47c0dba) |  |
| <a id="s-8401881f48"></a>`type` | yes | const="state_comparison" |  |

### <a id="s-e5941ad180"></a>definition `contentDescription`

- <a id="s-f0610b0c5b"></a>`type`: `"object"`
- <a id="s-0acf4cc5d1"></a>`additionalProperties`: `false`
- <a id="s-73ecb1f1f4"></a>`description`: `"Opaque primary byte-stream observations only: length and cryptographic fixity. Internal format, codec, dimensions, duration, and other byte semantics are outside the core profile."`
- <a id="s-7d1bbd47ed"></a>`required`: `["size_bytes","digests"]`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `digests` | yes | [See definition `contentDescription` · field `digests`](#s-d8a3c25d9d) |  |
| <a id="s-26c2b67719"></a>`size_bytes` | yes | type="integer"; minimum=0; maximum=9223372036854775807 |  |

### <a id="s-7cf5239b36"></a>definition `continuityBasis`

- <a id="s-53876aa66e"></a>`type`: `"object"`
- <a id="s-dd6eca0de4"></a>`additionalProperties`: `false`
- <a id="s-0c0d40499a"></a>`required`: `["type"]`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-bff05f4b5d"></a>`type` | yes | type="string"; enum=["native_file_identity","snapshot_identity","repository_tracking","process_record","operator_assertion","matching_fixity_and_locator","other"] |  |
| <a id="s-573918553e"></a>`uri` | no | [absoluteUri](#s-088a6da7a6) |  |

#### All must match (`allOf`)

| Rule | If schema matches | Then must match | Otherwise must match |
|---|---|---|---|
| <a id="s-2ef945dac5"></a>1 | properties={type: (const="other")}; required=["type"] | required=["uri"] | no additional constraint |

### <a id="s-37e4bccab2"></a>definition `continuityRelation`

- <a id="s-5094132d29"></a>`type`: `"object"`
- <a id="s-52faf9e52c"></a>`additionalProperties`: `false`
- <a id="s-d37e7341e3"></a>`required`: `["id","type","from_state","to_state","continuity_kind","basis","asserted_by_agent_id","confidence"]`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-9980b164f0"></a>`asserted_by_agent_id` | yes | [urnUuid](#s-b9f9301bba) |  |
| <a id="s-1396b618b3"></a>`basis` | yes | type="array"; items=([continuityBasis](#s-7cf5239b36)); minItems=1; uniqueItems=true |  |
| <a id="s-fe42ae1649"></a>`confidence` | yes | type="string"; enum=["high","medium","low","unknown"] |  |
| <a id="s-8084023ad5"></a>`continuity_kind` | yes | type="string"; enum=["reobservation","same_source_instance","declared_successor"] |  |
| <a id="s-6c9cbe3668"></a>`from_state` | yes | [stateReference](#s-6bc47c0dba) |  |
| <a id="s-f812d3e44a"></a>`id` | yes | [urnUuid](#s-b9f9301bba) |  |
| <a id="s-4dfdb76d76"></a>`note` | no | [nonEmptyString](#s-ddcf450d4d) |  |
| <a id="s-452a776527"></a>`to_state` | yes | [stateReference](#s-6bc47c0dba) |  |
| <a id="s-843d0b619c"></a>`type` | yes | const="state_continuity" |  |

### <a id="s-9293f3fd5e"></a>definition `correctionBody`

- <a id="s-57874fbe4f"></a>`type`: `"object"`
- <a id="s-5df8c10325"></a>`additionalProperties`: `false`
- <a id="s-a678a14fa7"></a>`description`: `"Monotonic correction of prior assertion entries. Supersession changes the effective provenance description; it never denotes a change to the file itself."`
- <a id="s-f071a5c1ee"></a>`required`: `["action","reason","supersedes"]`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-cdac9dca31"></a>`action` | yes | type="string"; enum=["replace","retract"] |  |
| <a id="s-10844b5a7f"></a>`reason` | yes | [nonEmptyString](#s-ddcf450d4d) |  |
| <a id="s-c080fe6439"></a>`replacement` | no | [graphFragment](#s-b3f23dc219) |  |
| <a id="s-49a4432a2c"></a>`supersedes` | yes | type="array"; items=([entryReference](#s-4a73d347fd)); minItems=1; uniqueItems=true |  |

#### All must match (`allOf`)

| Rule | If schema matches | Then must match | Otherwise must match |
|---|---|---|---|
| <a id="s-5471bcb7f8"></a>1 | properties={action: (const="replace")}; required=["action"] | required=["replacement"] | no additional constraint |
| <a id="s-ff047e2f58"></a>2 | properties={action: (const="retract")}; required=["action"] | not=(required=["replacement"]) | no additional constraint |

### <a id="s-ec32049096"></a>definition `coverage`

- <a id="s-df67c86640"></a>`type`: `"object"`
- <a id="s-572a9d7c53"></a>`additionalProperties`: `false`
- <a id="s-a2079cd6a6"></a>`description`: `"Exhaustiveness status for every filesystem capture category. Complete concerns accountable enumeration under the declared plan, not universal retention."`
- <a id="s-ed4d5dec09"></a>`required`: `["content_fixity","locator","basic_filesystem","timestamps","ownership","permissions","native_identifiers","extended_attributes","access_control","alternate_streams","resource_forks","file_flags","security_metadata","storage_layout","special_file_features","native_metadata_other"]`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-38ff5c8f50"></a>`access_control` | yes | [coverageStatus](#s-e778f73563) |  |
| <a id="s-fddb0819ed"></a>`alternate_streams` | yes | [coverageStatus](#s-e778f73563) |  |
| <a id="s-02b79b326a"></a>`basic_filesystem` | yes | const="complete" |  |
| <a id="s-d6dfaf26eb"></a>`content_fixity` | yes | const="complete" |  |
| <a id="s-5766061259"></a>`extended_attributes` | yes | [coverageStatus](#s-e778f73563) |  |
| <a id="s-d2e1c127e7"></a>`file_flags` | yes | [coverageStatus](#s-e778f73563) |  |
| <a id="s-cc0d71cc06"></a>`locator` | yes | const="complete" |  |
| <a id="s-254a86770b"></a>`native_identifiers` | yes | [coverageStatus](#s-e778f73563) |  |
| <a id="s-eeb6be3f87"></a>`native_metadata_other` | yes | [coverageStatus](#s-e778f73563) |  |
| <a id="s-c88d38015d"></a>`ownership` | yes | [coverageStatus](#s-e778f73563) |  |
| <a id="s-fd6804f94a"></a>`permissions` | yes | [coverageStatus](#s-e778f73563) |  |
| <a id="s-dd2b0b7939"></a>`resource_forks` | yes | [coverageStatus](#s-e778f73563) |  |
| <a id="s-491a25791c"></a>`security_metadata` | yes | [coverageStatus](#s-e778f73563) |  |
| <a id="s-82e87d95ee"></a>`special_file_features` | yes | [coverageStatus](#s-e778f73563) |  |
| <a id="s-32411ef94d"></a>`storage_layout` | yes | [coverageStatus](#s-e778f73563) |  |
| <a id="s-dcf2726f37"></a>`timestamps` | yes | [coverageStatus](#s-e778f73563) |  |

### <a id="s-e778f73563"></a>definition `coverageStatus`

- <a id="s-5acb30a80a"></a>`type`: `"string"`
- <a id="s-f907589f3c"></a>`enum`: `["complete","partial","not_supported","not_applicable","not_requested","failed"]`

### <a id="s-24acd86cba"></a>definition `decimalValue`

- <a id="s-ed9de302be"></a>`type`: `"object"`
- <a id="s-806d0fdb45"></a>`additionalProperties`: `false`
- <a id="s-9540708786"></a>`required`: `["type","data"]`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-ac02a1d7d0"></a>`data` | yes | type="string"; pattern="^-?(?:0\|[1-9][0-9]*)(?:\\.[0-9]+)?$" |  |
| <a id="s-379dcb7aca"></a>`type` | yes | const="decimal" |  |

### <a id="s-1d0efc506d"></a>definition `derivationRelation`

- <a id="s-5cd8948560"></a>`type`: `"object"`
- <a id="s-5293ae7d7e"></a>`additionalProperties`: `false`
- <a id="s-0001c73d8b"></a>`required`: `["id","type","generated_state","used_state","activity_id","derivation_kind"]`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-4682759be7"></a>`activity_id` | yes | [urnUuid](#s-b9f9301bba) |  |
| <a id="s-8b6089e88c"></a>`derivation_kind` | yes | type="string"; enum=["revision","transformation","copy","metadata_change","relocation","aggregation","extraction","other"] |  |
| <a id="s-c4b2858983"></a>`derivation_kind_uri` | no | [absoluteUri](#s-088a6da7a6) |  |
| <a id="s-7139ecf258"></a>`generated_state` | yes | [stateReference](#s-6bc47c0dba) |  |
| <a id="s-b11ac70356"></a>`id` | yes | [urnUuid](#s-b9f9301bba) |  |
| <a id="s-294c272298"></a>`type` | yes | const="derivation" |  |
| <a id="s-c18d649ef0"></a>`used_state` | yes | [stateReference](#s-6bc47c0dba) |  |

#### All must match (`allOf`)

| Rule | If schema matches | Then must match | Otherwise must match |
|---|---|---|---|
| <a id="s-a5933f03da"></a>1 | properties={derivation_kind: (const="other")}; required=["derivation_kind"] | required=["derivation_kind_uri"] | no additional constraint |

### <a id="s-5eb1052892"></a>definition `diagnostic`

- <a id="s-098c320097"></a>`type`: `"object"`
- <a id="s-6238b77731"></a>`additionalProperties`: `false`
- <a id="s-fca2888238"></a>`required`: `["severity","category","code","message"]`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-b5d2a36546"></a>`category` | yes | type="string"; enum=["general","content_fixity","locator","basic_filesystem","timestamps","ownership","permissions","native_identifiers","extended_attributes","access_control","alternate_streams","resource_forks","file_flags","security_metadata","storage_layout","special_file_features","native_metadata_other"] |  |
| <a id="s-aca6a83557"></a>`code` | yes | type="string"; maxLength=255; minLength=1; pattern="^[^\\u0000\\uD800-\\uDFFF]+$" |  |
| <a id="s-6e2daa0b74"></a>`message` | yes | [nonEmptyString](#s-ddcf450d4d) |  |
| <a id="s-90ecc6e3d2"></a>`native_code` | no | [nonEmptyString](#s-ddcf450d4d) |  |
| <a id="s-a7b8f4cfbd"></a>`severity` | yes | type="string"; enum=["warning","error"] |  |
| <a id="s-e2675fb693"></a>`source` | no | [sourceDescriptor](#s-6bcbe0dfe1) |  |

### <a id="s-1e1e5e54b0"></a>definition `digest`

- <a id="s-5c9738cad5"></a>`type`: `"object"`
- <a id="s-78ad4f9896"></a>`additionalProperties`: `false`
- <a id="s-fe5e8ea8d1"></a>`description`: `"A cryptographic digest assertion. Content digests identify byte equality evidence, not file-entity identity."`
- <a id="s-fa0ca9aee9"></a>`required`: `["algorithm","encoding","value","purpose","originator_agent_id"]`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-5a1578fd7e"></a>`algorithm` | yes | type="string"; pattern="^[a-z0-9][a-z0-9.-]{0,63}$" |  |
| <a id="s-74105c73b7"></a>`algorithm_uri` | no | [absoluteUri](#s-088a6da7a6) |  |
| <a id="s-948704513c"></a>`encoding` | yes | const="hex" |  |
| <a id="s-bbeefbd4af"></a>`originator_agent_id` | yes | [urnUuid](#s-b9f9301bba) |  |
| <a id="s-1a3f525701"></a>`purpose` | yes | type="string"; enum=["fixity","native_metadata","executable","configuration","evidence","journal_entry","other"] |  |
| <a id="s-9c65c713b7"></a>`purpose_uri` | no | [absoluteUri](#s-088a6da7a6) |  |
| <a id="s-7339d6a4c2"></a>`value` | yes | type="string"; pattern="^[0-9a-f]+$" |  |

#### All must match (`allOf`)

| Rule | If schema matches | Then must match | Otherwise must match |
|---|---|---|---|
| <a id="s-993f370b79"></a>1 | properties={algorithm: (const="sha-256")}; required=["algorithm"] | properties={value: (type="string"; pattern="^[0-9a-f]{64}$")} | no additional constraint |
| <a id="s-765441b45e"></a>2 | properties={algorithm: (const="sha-512")}; required=["algorithm"] | properties={value: (type="string"; pattern="^[0-9a-f]{128}$")} | no additional constraint |
| <a id="s-6b24dfbe3f"></a>3 | properties={algorithm: (not=(enum=["sha-256","sha-512"]))}; required=["algorithm"] | required=["algorithm_uri"] | no additional constraint |
| <a id="s-03c58f0410"></a>4 | properties={purpose: (const="other")}; required=["purpose"] | required=["purpose_uri"] | no additional constraint |

### <a id="s-b87a395047"></a>definition `digestOnlyValue`

- <a id="s-cbf1020437"></a>`type`: `"object"`
- <a id="s-5959eaae3c"></a>`additionalProperties`: `false`
- <a id="s-445e06d663"></a>`required`: `["type","byte_length","digests"]`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-c7446adbb4"></a>`byte_length` | yes | type="integer"; minimum=0; maximum=9223372036854775807 |  |
| <a id="s-bf0abcdad0"></a>`digests` | yes | type="array"; items=([digest](#s-1e1e5e54b0)); minItems=1; uniqueItems=true |  |
| <a id="s-8ce3458134"></a>`type` | yes | const="digest" |  |

### <a id="s-47030d684d"></a>definition `entityReferenceValue`

- <a id="s-cd33351ffa"></a>`type`: `"object"`
- <a id="s-ff866afe4d"></a>`additionalProperties`: `false`
- <a id="s-e5434a739f"></a>`required`: `["type","data"]`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-73e2b75021"></a>`data` | yes | [absoluteUri](#s-088a6da7a6) |  |
| <a id="s-54dbfc088e"></a>`type` | yes | const="entity_reference" |  |

### <a id="s-4a73d347fd"></a>definition `entryReference`

- <a id="s-9a7012f756"></a>`type`: `"object"`
- <a id="s-30d6b3a2a3"></a>`additionalProperties`: `false`
- <a id="s-d3adc99ceb"></a>`description`: `"Reference to the exact UTF-8 JSON-text octets of an earlier journal entry, excluding RFC 7464 RS/LF framing."`
- <a id="s-e7ca33ba9a"></a>`required`: `["entry_id","sequence","json_sha256"]`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-6d5bc1dc9b"></a>`entry_id` | yes | [urnUuid](#s-b9f9301bba) |  |
| <a id="s-0a4a983200"></a>`json_sha256` | yes | [sha256Hex](#s-1ceccb1b61) |  |
| <a id="s-f4eef91776"></a>`sequence` | yes | type="integer"; minimum=0; maximum=9223372036854775807 |  |

### <a id="s-20b7cca542"></a>definition `environment`

- <a id="s-6a674bb08d"></a>`type`: `"object"`
- <a id="s-730d512d86"></a>`additionalProperties`: `false`
- <a id="s-bbe9947543"></a>`description`: `"Host/OS/filesystem/runtime state used by an observation or transition; a PROV Entity, not an Agent merely because execution occurred there."`
- <a id="s-d8efe274c5"></a>`required`: `["id","type","host","operating_system","filesystem","runtime"]`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-f7e413acd8"></a>`filesystem` | yes | [filesystem](#s-92f8e87e33) |  |
| <a id="s-8fb236aae6"></a>`host` | yes | [host](#s-03a9361935) |  |
| <a id="s-241888f780"></a>`id` | yes | [urnUuid](#s-b9f9301bba) |  |
| <a id="s-db0046ab62"></a>`operating_system` | yes | [operatingSystem](#s-e585eafb76) |  |
| <a id="s-08b7e46c26"></a>`runtime` | yes | [runtime](#s-a011de07f6) |  |
| <a id="s-b21bca7820"></a>`type` | yes | const="technical_environment" |  |

### <a id="s-dd3ca68abf"></a>definition `fieldSourceDescriptor`


#### All must match (`allOf`)

| Alternative | Schema |
|---|---|
| <a id="s-827ce32353"></a>1 | [sourceDescriptor](#s-6bcbe0dfe1) |
| <a id="s-d77a85f20b"></a>2 | required=["field"] |

### <a id="s-42ed3b06f0"></a>definition `fileLineage`

- <a id="s-5dac8a3781"></a>`type`: `"object"`
- <a id="s-0674beda5a"></a>`additionalProperties`: `false`
- <a id="s-5b156f040d"></a>`description`: `"An abstract continuing file lineage. Each immutable state is a specialization of one lineage; grouping is an attributable continuity assertion, not inferred from path or digest alone."`
- <a id="s-32b45487c9"></a>`required`: `["id","type","continuity_basis","asserted_by_agent_id"]`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-f09f706cd4"></a>`asserted_by_agent_id` | yes | [urnUuid](#s-b9f9301bba) |  |
| <a id="s-0a452a3e7d"></a>`continuity_basis` | yes | type="string"; enum=["producer_declared","repository_tracking","source_native_tracking","process_provenance","other"] |  |
| <a id="s-8e083ba9be"></a>`continuity_basis_uri` | no | [absoluteUri](#s-088a6da7a6) |  |
| <a id="s-6afa6093f4"></a>`id` | yes | [urnUuid](#s-b9f9301bba) |  |
| <a id="s-0d3b23ad9d"></a>`identifiers` | no | type="array"; items=([identifier](#s-e6d4c3ce94)); minItems=1; uniqueItems=true |  |
| <a id="s-1064150972"></a>`label` | no | [nonEmptyString](#s-ddcf450d4d) |  |
| <a id="s-7ea2d17ae9"></a>`notes` | no | type="array"; items=([nonEmptyString](#s-ddcf450d4d)); minItems=1; uniqueItems=true |  |
| <a id="s-858280e078"></a>`type` | yes | const="file_lineage" |  |

#### All must match (`allOf`)

| Rule | If schema matches | Then must match | Otherwise must match |
|---|---|---|---|
| <a id="s-6d84c62b8d"></a>1 | properties={continuity_basis: (const="other")}; required=["continuity_basis"] | required=["continuity_basis_uri"] | no additional constraint |

### <a id="s-72ed1caeab"></a>definition `fileState`

- <a id="s-88f2cf8b3c"></a>`type`: `"object"`
- <a id="s-2e5f4e27ed"></a>`additionalProperties`: `false`
- <a id="s-496df884ad"></a>`description`: `"One immutable, full observed snapshot of one operating-system regular-file state; a PROV Entity and PREMIS File."`
- <a id="s-aca7b96f5c"></a>`required`: `["id","type","lineage_id","locator","content","filesystem_metadata"]`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-dc97b6e82c"></a>`content` | yes | [contentDescription](#s-e5941ad180) |  |
| <a id="s-f4486944fe"></a>`filesystem_metadata` | yes | [filesystemMetadata](#s-2c6dad9ae6) |  |
| <a id="s-b880971bdc"></a>`id` | yes | [urnUuid](#s-b9f9301bba) |  |
| <a id="s-352feecf8f"></a>`lineage_id` | yes | [urnUuid](#s-b9f9301bba) |  |
| <a id="s-7a72296a2a"></a>`locator` | yes | [locator](#s-334ec6328c) |  |
| <a id="s-8a8b9a8e50"></a>`notes` | no | type="array"; items=([nonEmptyString](#s-ddcf450d4d)); minItems=1; uniqueItems=true |  |
| <a id="s-61ced83dca"></a>`type` | yes | const="regular_file_state" |  |

### <a id="s-92f8e87e33"></a>definition `filesystem`

- <a id="s-19c4583537"></a>`type`: `"object"`
- <a id="s-dbc57839c4"></a>`additionalProperties`: `false`
- <a id="s-454bea42bf"></a>`required`: `["type"]`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-7d65f8f0fb"></a>`case_preserving` | no | type="boolean" |  |
| <a id="s-a8b4b18f4d"></a>`case_sensitive` | no | type="boolean" |  |
| <a id="s-fbf1db8700"></a>`mount_locator` | no | [locator](#s-334ec6328c) |  |
| <a id="s-aa28a5f777"></a>`name_normalization` | no | type="string"; enum=["none","nfc","nfd","nfkc","nfkd","implementation_defined","unknown"] |  |
| <a id="s-0f3f1d9c98"></a>`networked` | no | type="boolean" |  |
| <a id="s-3fa7d05459"></a>`snapshot_identifiers` | no | type="array"; items=([identifier](#s-e6d4c3ce94)); minItems=1; uniqueItems=true |  |
| <a id="s-9f1b983483"></a>`type` | yes | type="string"; maxLength=128; minLength=1; pattern="^[^\\u0000\\uD800-\\uDFFF]+$" |  |
| <a id="s-660018dfdc"></a>`type_uri` | no | [absoluteUri](#s-088a6da7a6) |  |
| <a id="s-695c44a803"></a>`version` | no | [nonEmptyString](#s-ddcf450d4d) |  |
| <a id="s-93f4226725"></a>`volume_identifiers` | no | type="array"; items=([identifier](#s-e6d4c3ce94)); minItems=1; uniqueItems=true |  |

### <a id="s-2c6dad9ae6"></a>definition `filesystemMetadata`

- <a id="s-502a72fdcb"></a>`type`: `"object"`
- <a id="s-7a715d2c0c"></a>`additionalProperties`: `false`
- <a id="s-4e926ba4e6"></a>`required`: `["timestamps","native_identifiers","native_metadata"]`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-4e7cc91131"></a>`access` | no | [accessMetadata](#s-f7179075a7) |  |
| <a id="s-85b269c7a3"></a>`native_identifiers` | yes | type="array"; items=([observedIdentifier](#s-325bfde37a)); uniqueItems=true |  |
| <a id="s-6957b5d6cb"></a>`native_metadata` | yes | type="array"; items=([nativeMetadata](#s-29df5b2baf)); uniqueItems=true |  |
| <a id="s-10ce445ffb"></a>`timestamps` | yes | type="array"; items=([timestampObservation](#s-f1f3653cef)); uniqueItems=true |  |

### <a id="s-fd99a1f0e8"></a>definition `generationRelation`

- <a id="s-1351017b9a"></a>`type`: `"object"`
- <a id="s-18f3d4d137"></a>`additionalProperties`: `false`
- <a id="s-9547c27181"></a>`required`: `["id","type","activity_id","state","role"]`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-ab8ffa0d26"></a>`activity_id` | yes | [urnUuid](#s-b9f9301bba) |  |
| <a id="s-73ec096284"></a>`id` | yes | [urnUuid](#s-b9f9301bba) |  |
| <a id="s-0818e792f0"></a>`role` | yes | [relationRole](#s-e8775f45df) |  |
| <a id="s-391a39769e"></a>`role_uri` | no | [absoluteUri](#s-088a6da7a6) |  |
| <a id="s-fc03a83fbc"></a>`state` | yes | [stateReference](#s-6bc47c0dba) |  |
| <a id="s-5bb1cedbcc"></a>`type` | yes | const="generation" |  |

#### All must match (`allOf`)

| Rule | If schema matches | Then must match | Otherwise must match |
|---|---|---|---|
| <a id="s-164b3c14fe"></a>1 | properties={role: (const="other")}; required=["role"] | required=["role_uri"] | no additional constraint |

### <a id="s-b3f23dc219"></a>definition `graphFragment`

- <a id="s-989663d5cf"></a>`type`: `"object"`
- <a id="s-25f27598d0"></a>`additionalProperties`: `false`
- <a id="s-89b9658ab2"></a>`description`: `"An immutable atomic bundle of graph assertions. Arrays are semantic sets; array order does not imply chronology or causality."`
- <a id="s-2fea751209"></a>`minProperties`: `1`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-4bc7c3210b"></a>`activities` | no | type="array"; items=([transitionActivity](#s-95e37398de)); minItems=1; uniqueItems=true |  |
| <a id="s-f8492f18ad"></a>`agents` | no | type="array"; items=([agent](#s-73d177dbf5)); minItems=1; uniqueItems=true |  |
| <a id="s-08b1a0dcef"></a>`captures` | no | type="array"; items=([captureEvent](#s-67bdd25fe2)); minItems=1; uniqueItems=true |  |
| <a id="s-3fdd6cd86d"></a>`environments` | no | type="array"; items=([environment](#s-20b7cca542)); minItems=1; uniqueItems=true |  |
| <a id="s-c242ca70cf"></a>`extensions` | no | type="array"; items=([semanticAssertion](#s-2d2165b5a6)); minItems=1; uniqueItems=true |  |
| <a id="s-951061ad06"></a>`lineages` | no | type="array"; items=([fileLineage](#s-42ed3b06f0)); minItems=1; uniqueItems=true |  |
| <a id="s-f7fd60d745"></a>`payload_bindings` | no | type="array"; items=([payloadBinding](#s-86c27faf36)); minItems=1; uniqueItems=true |  |
| <a id="s-02ccd37f77"></a>`relations` | no | type="array"; items=([relation](#s-a65a100c0b)); minItems=1; uniqueItems=true |  |
| <a id="s-dc1fadd69b"></a>`states` | no | type="array"; items=([fileState](#s-72ed1caeab)); minItems=1; uniqueItems=true |  |

### <a id="s-2d4ca24d8e"></a>definition `hardwareAgent`

- <a id="s-03490c4b5b"></a>`type`: `"object"`
- <a id="s-c2e22a7ad9"></a>`additionalProperties`: `false`
- <a id="s-b57cea77b3"></a>`required`: `["id","type","name"]`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-d05455a580"></a>`id` | yes | [urnUuid](#s-b9f9301bba) |  |
| <a id="s-08e920634a"></a>`identifiers` | no | type="array"; items=([identifier](#s-e6d4c3ce94)); minItems=1; uniqueItems=true |  |
| <a id="s-63320319b9"></a>`model` | no | [nonEmptyString](#s-ddcf450d4d) |  |
| <a id="s-c3e0bc97a2"></a>`name` | yes | [nonEmptyString](#s-ddcf450d4d) |  |
| <a id="s-7d70bf5735"></a>`type` | yes | const="hardware" |  |
| <a id="s-936321025e"></a>`vendor` | no | [nonEmptyString](#s-ddcf450d4d) |  |
| <a id="s-045f815685"></a>`version` | no | [nonEmptyString](#s-ddcf450d4d) |  |

### <a id="s-03a9361935"></a>definition `host`

- <a id="s-bf79215119"></a>`type`: `"object"`
- <a id="s-d442fd6130"></a>`additionalProperties`: `false`
- <a id="s-d285ccbaa6"></a>`required`: `["id"]`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-363883e7fc"></a>`hardware_architecture` | no | [nonEmptyString](#s-ddcf450d4d) |  |
| <a id="s-0b58643975"></a>`hardware_model` | no | [nonEmptyString](#s-ddcf450d4d) |  |
| <a id="s-98ccb3816c"></a>`id` | yes | [urnUuid](#s-b9f9301bba) |  |
| <a id="s-f69daf1102"></a>`identifiers` | no | type="array"; items=([identifier](#s-e6d4c3ce94)); minItems=1; uniqueItems=true |  |
| <a id="s-087b7a6d2c"></a>`name` | no | [nonEmptyString](#s-ddcf450d4d) |  |

### <a id="s-e6d4c3ce94"></a>definition `identifier`

- <a id="s-7d560a0e46"></a>`type`: `"object"`
- <a id="s-c82bf555d0"></a>`additionalProperties`: `false`
- <a id="s-34051a58e2"></a>`required`: `["scheme","value","scope","representation"]`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-d5d56c80f4"></a>`authority_id` | no | [absoluteUri](#s-088a6da7a6) |  |
| <a id="s-9c6227dca7"></a>`representation` | yes | type="string"; enum=["clear","sha256","hmac_sha256"] |  |
| <a id="s-7b8bf71694"></a>`scheme` | yes | type="string"; maxLength=255; minLength=1; pattern="^[^\\u0000\\uD800-\\uDFFF]+$" |  |
| <a id="s-7eca821997"></a>`scope` | yes | type="string"; enum=["record","repository","host","volume","filesystem","organization","global"] |  |
| <a id="s-10c039f6c2"></a>`value` | yes | type="string"; minLength=1; pattern="^[^\\u0000\\uD800-\\uDFFF]+$" |  |

#### All must match (`allOf`)

| Rule | If schema matches | Then must match | Otherwise must match |
|---|---|---|---|
| <a id="s-86b21d23fb"></a>1 | properties={representation: (enum=["sha256","hmac_sha256"])}; required=["representation"] | properties={value: (type="string"; pattern="^[0-9a-f]{64}$")} | no additional constraint |
| <a id="s-1219ff0cfc"></a>2 | properties={scope: (not=(const="global"))}; required=["scope"] | required=["authority_id"] | no additional constraint |
| <a id="s-1d2e8ebab8"></a>3 | properties={representation: (const="hmac_sha256")}; required=["representation"] | required=["authority_id"] | no additional constraint |

### <a id="s-e7a29598b5"></a>definition `integerValue`

- <a id="s-e93c720870"></a>`type`: `"object"`
- <a id="s-e015db0a91"></a>`additionalProperties`: `false`
- <a id="s-842c1413f4"></a>`required`: `["type","data"]`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-1295cf55d5"></a>`data` | yes | type="string"; pattern="^-?(?:0\|[1-9][0-9]*)$" |  |
| <a id="s-9672d7173d"></a>`type` | yes | const="integer" |  |

### <a id="s-f201133ac4"></a>definition `interpretation`

- <a id="s-2bbfdee7ed"></a>`type`: `"object"`
- <a id="s-f49cceed96"></a>`additionalProperties`: `false`
- <a id="s-be63e1aed7"></a>`required`: `["kind","value","agent_id","confidence"]`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-de319a9575"></a>`agent_id` | yes | [urnUuid](#s-b9f9301bba) |  |
| <a id="s-719fc36eac"></a>`confidence` | yes | type="string"; enum=["high","medium","low","unknown"] |  |
| <a id="s-7bf9b5ba70"></a>`kind` | yes | type="string"; enum=["text_decode","structured_parse","normalized_value","other"] |  |
| <a id="s-c24ed7c699"></a>`note` | no | [nonEmptyString](#s-ddcf450d4d) |  |
| <a id="s-580da34e34"></a>`schema` | no | [absoluteUri](#s-088a6da7a6) |  |
| <a id="s-fd385f9ac8"></a>`value` | yes | [capturedValue](#s-14ff4aeeaf) |  |

#### All must match (`allOf`)

| Rule | If schema matches | Then must match | Otherwise must match |
|---|---|---|---|
| <a id="s-9f57090ac1"></a>1 | properties={kind: (const="text_decode")}; required=["kind"] | properties={value: ([textValue](#s-ce6e164ba2))} | no additional constraint |
| <a id="s-592f755ce9"></a>2 | properties={kind: (enum=["structured_parse","other"])}; required=["kind"] | required=["schema"] | no additional constraint |

### <a id="s-7be03ea942"></a>definition `invalidationRelation`

- <a id="s-b423833c0d"></a>`type`: `"object"`
- <a id="s-6076522df0"></a>`additionalProperties`: `false`
- <a id="s-dbb6336937"></a>`required`: `["id","type","activity_id","state","reason"]`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-c513fb5b0e"></a>`activity_id` | yes | [urnUuid](#s-b9f9301bba) |  |
| <a id="s-6d8344fe5f"></a>`id` | yes | [urnUuid](#s-b9f9301bba) |  |
| <a id="s-82cfecd288"></a>`reason` | yes | [nonEmptyString](#s-ddcf450d4d) |  |
| <a id="s-70da06969b"></a>`state` | yes | [stateReference](#s-6bc47c0dba) |  |
| <a id="s-833c5b44f7"></a>`type` | yes | const="invalidation" |  |

### <a id="s-8b0ea68958"></a>definition `journalInitBody`

- <a id="s-1f0120cdf8"></a>`type`: `"object"`
- <a id="s-7e4a29bbba"></a>`additionalProperties`: `false`
- <a id="s-689e8aaa5e"></a>`required`: `["journal","assertions"]`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-0fad3e6c15"></a>`assertions` | yes | [graphFragment](#s-b3f23dc219) |  |
| <a id="s-8233fca1f0"></a>`journal` | yes | [journalPolicy](#s-31e80eaaa2) |  |

### <a id="s-31e80eaaa2"></a>definition `journalPolicy`

- <a id="s-48586b557c"></a>`type`: `"object"`
- <a id="s-f35d074cbe"></a>`additionalProperties`: `false`
- <a id="s-70f25cbea5"></a>`required`: `["primary_lineage_id","scope","serialization","entry_digest_algorithm","entry_digest_coverage","state_representation","payload_semantics","correction_model","retention_intent"]`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-f5264e4c25"></a>`correction_model` | yes | const="monotonic_entry_supersession" |  |
| <a id="s-f6d11eeb17"></a>`entry_digest_algorithm` | yes | const="sha-256" |  |
| <a id="s-8cbf08550e"></a>`entry_digest_coverage` | yes | const="json_text_octets_excluding_framing" |  |
| <a id="s-4a9f7755bf"></a>`label` | no | [nonEmptyString](#s-ddcf450d4d) |  |
| <a id="s-dd0b77ab3d"></a>`payload_semantics` | yes | const="opaque_bytes" |  |
| <a id="s-33a5ec6512"></a>`primary_lineage_id` | yes | [urnUuid](#s-b9f9301bba) |  |
| <a id="s-fd7f820f59"></a>`retention_intent` | yes | type="string"; enum=["permanent_archival","long_term_preservation","operational_provenance","other"] |  |
| <a id="s-24e941b8ac"></a>`retention_intent_uri` | no | [absoluteUri](#s-088a6da7a6) |  |
| <a id="s-bb25031b39"></a>`scope` | yes | const="primary_lineage_with_related_provenance" |  |
| <a id="s-d151f85da1"></a>`serialization` | yes | const="rfc7464_json_text_sequence" |  |
| <a id="s-587b0cd23c"></a>`state_representation` | yes | const="full_snapshot" |  |

#### All must match (`allOf`)

| Rule | If schema matches | Then must match | Otherwise must match |
|---|---|---|---|
| <a id="s-dc94be5970"></a>1 | properties={retention_intent: (const="other")}; required=["retention_intent"] | required=["retention_intent_uri"] | no additional constraint |

### <a id="s-461d5ccad6"></a>definition `jsonValue`

- <a id="s-3a6a299674"></a>`type`: `"object"`
- <a id="s-ec1f488883"></a>`additionalProperties`: `false`
- <a id="s-8a18b93157"></a>`required`: `["type","data","schema"]`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-0c5b32161d"></a>`data` | yes | [nonNullJson](#s-899e812405) |  |
| <a id="s-f69ea1da80"></a>`schema` | yes | [absoluteUri](#s-088a6da7a6) |  |
| <a id="s-f4efd63ff1"></a>`type` | yes | const="json" |  |

### <a id="s-2920056f4c"></a>definition `kernel`

- <a id="s-5665c307e4"></a>`type`: `"object"`
- <a id="s-3d967db013"></a>`additionalProperties`: `false`
- <a id="s-5fe76f6aa0"></a>`minProperties`: `1`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-c2f2f3389a"></a>`name` | no | [nonEmptyString](#s-ddcf450d4d) |  |
| <a id="s-ac4e5f9b14"></a>`release` | no | [nonEmptyString](#s-ddcf450d4d) |  |
| <a id="s-3246163f46"></a>`version` | no | [nonEmptyString](#s-ddcf450d4d) |  |

### <a id="s-334ec6328c"></a>definition `locator`

- <a id="s-01dba8b219"></a>`type`: `"object"`
- <a id="s-d2c3d4fd65"></a>`additionalProperties`: `false`
- <a id="s-5981d1cfcb"></a>`required`: `["syntax","kind"]`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-0c4cd5ac17"></a>`authority_id` | no | [absoluteUri](#s-088a6da7a6) |  |
| <a id="s-4fa7fe6803"></a>`bytes` | no | [byteString](#s-f8865efa4d) |  |
| <a id="s-973a02b839"></a>`kind` | yes | type="string"; enum=["absolute","relative","opaque"] |  |
| <a id="s-f2eb746e64"></a>`source_encoding` | no | [nonEmptyString](#s-ddcf450d4d) |  |
| <a id="s-b2b4d41374"></a>`syntax` | yes | type="string"; enum=["posix","windows","uri","opaque"] |  |
| <a id="s-dde7f08327"></a>`text` | no | [portableString](#s-94d0b65f81) |  |
| <a id="s-bd70fe6263"></a>`text_role` | no | type="string"; enum=["exact","display"] | Required when text and bytes coexist. |

#### All must match (`allOf`)

| Rule | If schema matches | Then must match | Otherwise must match |
|---|---|---|---|
| <a id="s-1f8f62b61b"></a>1 | properties={syntax: (const="opaque")}; required=["syntax"] | properties={kind: (const="opaque")} | no additional constraint |
| <a id="s-f07b186968"></a>2 | properties={kind: (const="opaque")}; required=["kind"] | properties={syntax: (const="opaque")} | no additional constraint |
| <a id="s-f74afbe90a"></a>3 | required=["text_role"] | required=["text"] | no additional constraint |
| <a id="s-a5792e3fbc"></a>4 | properties={text_role: (const="display")}; required=["text_role"] | required=["bytes"] | no additional constraint |
| <a id="s-a8aabfbd28"></a>5 | required=["text","bytes"] | required=["text_role"] | no additional constraint |
| <a id="s-257c4e9c2e"></a>6 | properties={text_role: (const="exact")}; required=["text_role","bytes"] | required=["source_encoding"] | no additional constraint |

#### At least one must match (`anyOf`)

| Alternative | Schema |
|---|---|
| <a id="s-a27a7e29e4"></a>1 | required=["text"] |
| <a id="s-b743166651"></a>2 | required=["bytes"] |

### <a id="s-cc468e7930"></a>definition `nativeCoverageCategory`

- <a id="s-e993697fc5"></a>`type`: `"string"`
- <a id="s-b609ba7426"></a>`enum`: `["extended_attributes","access_control","alternate_streams","resource_forks","file_flags","security_metadata","storage_layout","special_file_features","native_metadata_other"]`

### <a id="s-29df5b2baf"></a>definition `nativeMetadata`

- <a id="s-1ebdba8b37"></a>`type`: `"object"`
- <a id="s-283dae748b"></a>`additionalProperties`: `false`
- <a id="s-b2838b22b1"></a>`description`: `"Source-native metadata evidence separated from any interpretation and from restoration semantics."`
- <a id="s-3ed50d70b9"></a>`required`: `["kind","coverage_category","name","capture_status","source","sensitivity"]`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-70aeb35589"></a>`capture_status` | yes | type="string"; enum=["captured","digest_only","redacted","unreadable","not_retained"] |  |
| <a id="s-3fb66dad49"></a>`coverage_category` | yes | [nativeCoverageCategory](#s-cc468e7930) |  |
| <a id="s-e99672219a"></a>`interpretations` | no | type="array"; items=([interpretation](#s-f201133ac4)); minItems=1; uniqueItems=true |  |
| <a id="s-d93beeb529"></a>`kind` | yes | type="string"; enum=["extended_attribute","windows_extended_attribute","alternate_data_stream","resource_fork","finder_info","acl","security_descriptor","file_flag","capability","security_label","reparse_point","sparse_map","compression_state","encryption_state","native_stat_field","other"] |  |
| <a id="s-317f2d8b78"></a>`kind_uri` | no | [absoluteUri](#s-088a6da7a6) |  |
| <a id="s-f7c2f14eb0"></a>`name` | yes | [nonEmptyString](#s-ddcf450d4d) |  |
| <a id="s-33f8a1efaa"></a>`name_bytes` | no | [byteString](#s-f8865efa4d) |  |
| <a id="s-d730fabc1d"></a>`name_role` | no | type="string"; enum=["exact","display"] |  |
| <a id="s-208d9c80d2"></a>`name_source_encoding` | no | [nonEmptyString](#s-ddcf450d4d) |  |
| <a id="s-e081154ca2"></a>`namespace` | no | [nonEmptyString](#s-ddcf450d4d) |  |
| <a id="s-d68af5217c"></a>`note` | no | [nonEmptyString](#s-ddcf450d4d) |  |
| <a id="s-b98cbebd2e"></a>`observed_byte_length` | no | type="integer"; minimum=0; maximum=9223372036854775807 |  |
| <a id="s-fe33cc8571"></a>`sensitivity` | yes | type="string"; enum=["public","personal","security_sensitive","secret","unknown"] |  |
| <a id="s-9e9ce60318"></a>`source` | yes | [sourceDescriptor](#s-6bcbe0dfe1) |  |
| <a id="s-ee0882dca1"></a>`value` | no | [typedValue](#s-de01b50b5c) |  |

#### All must match (`allOf`)

| Rule | If schema matches | Then must match | Otherwise must match |
|---|---|---|---|
| <a id="s-a7798a9bed"></a>1 | properties={kind: (const="other")}; required=["kind"] | required=["kind_uri"] | no additional constraint |
| <a id="s-a06e19cf33"></a>2 | properties={capture_status: (const="captured")}; required=["capture_status"] | properties={value: ([capturedValue](#s-14ff4aeeaf))}; required=["value"] | no additional constraint |
| <a id="s-bd1e3a00b9"></a>3 | properties={capture_status: (const="digest_only")}; required=["capture_status"] | properties={value: ([digestOnlyValue](#s-b87a395047))}; required=["value"] | no additional constraint |
| <a id="s-d61668d7bc"></a>4 | properties={capture_status: (enum=["redacted","unreadable","not_retained"])}; required=["capture_status"] | not=(required=["value"]) | no additional constraint |
| <a id="s-b737d898fd"></a>5 | required=["name_bytes"] | required=["name_role"] | no additional constraint |
| <a id="s-275095a821"></a>6 | properties={name_role: (const="display")}; required=["name_role"] | required=["name_bytes"] | no additional constraint |
| <a id="s-a92e870fcd"></a>7 | properties={name_role: (const="exact")}; required=["name_role","name_bytes"] | required=["name_source_encoding"] | no additional constraint |
| <a id="s-96c75d4f3f"></a>8 | properties={capture_status: (enum=["redacted","unreadable","not_retained"])}; required=["capture_status"] | required=["note"] | no additional constraint |

### <a id="s-ddcf450d4d"></a>definition `nonEmptyString`

- <a id="s-cc77cb806f"></a>`type`: `"string"`
- <a id="s-7a032842dd"></a>`minLength`: `1`
- <a id="s-cf8a10685a"></a>`pattern`: `"^[^\\u0000\\uD800-\\uDFFF]+$"`

### <a id="s-899e812405"></a>definition `nonNullJson`


#### At least one must match (`anyOf`)

| Alternative | Schema |
|---|---|
| <a id="s-204721cf5b"></a>1 | [portableString](#s-94d0b65f81) |
| <a id="s-02e66dc14c"></a>2 | type="number" |
| <a id="s-434002e64c"></a>3 | type="boolean" |
| <a id="s-4bfccb267e"></a>4 | type="array"; items=([nonNullJson](#s-899e812405)) |
| <a id="s-b8d3cf55bd"></a>5 | type="object"; additionalProperties=([nonNullJson](#s-899e812405)); propertyNames=([portableString](#s-94d0b65f81)) |

### <a id="s-325bfde37a"></a>definition `observedIdentifier`

- <a id="s-00d788ece0"></a>`type`: `"object"`
- <a id="s-840e1721ba"></a>`additionalProperties`: `false`
- <a id="s-324ac7f159"></a>`required`: `["scheme","value","scope","representation","source"]`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-c3ff376e71"></a>`authority_id` | no | [absoluteUri](#s-088a6da7a6) |  |
| <a id="s-99ee85b3b8"></a>`representation` | yes | type="string"; enum=["clear","sha256","hmac_sha256"] |  |
| <a id="s-7ffa2e848b"></a>`scheme` | yes | type="string"; maxLength=255; minLength=1; pattern="^[^\\u0000\\uD800-\\uDFFF]+$" |  |
| <a id="s-071365fb05"></a>`scope` | yes | type="string"; enum=["record","repository","host","volume","filesystem","organization","global"] |  |
| <a id="s-a67988e41f"></a>`source` | yes | [sourceDescriptor](#s-6bcbe0dfe1) |  |
| <a id="s-cace1925b2"></a>`value` | yes | type="string"; minLength=1; pattern="^[^\\u0000\\uD800-\\uDFFF]+$" |  |

#### All must match (`allOf`)

| Rule | If schema matches | Then must match | Otherwise must match |
|---|---|---|---|
| <a id="s-5ed40337a8"></a>1 | properties={representation: (enum=["sha256","hmac_sha256"])}; required=["representation"] | properties={value: (type="string"; pattern="^[0-9a-f]{64}$")} | no additional constraint |
| <a id="s-013ed4bf99"></a>2 | properties={scope: (not=(const="global"))}; required=["scope"] | required=["authority_id"] | no additional constraint |
| <a id="s-cba3a4ee70"></a>3 | properties={representation: (const="hmac_sha256")}; required=["representation"] | required=["authority_id"] | no additional constraint |

### <a id="s-e585eafb76"></a>definition `operatingSystem`

- <a id="s-7a19420811"></a>`type`: `"object"`
- <a id="s-e612ac7189"></a>`additionalProperties`: `false`
- <a id="s-0a62118350"></a>`required`: `["family","name","version"]`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-7166b849f8"></a>`build` | no | [nonEmptyString](#s-ddcf450d4d) |  |
| <a id="s-46f57894fe"></a>`family` | yes | type="string"; enum=["linux","windows","macos","freebsd","openbsd","netbsd","illumos","aix","android","ios","solaris","other"] |  |
| <a id="s-8ec8a58fec"></a>`family_name` | no | [nonEmptyString](#s-ddcf450d4d) |  |
| <a id="s-9b913d3cb1"></a>`identifiers` | no | type="array"; items=([identifier](#s-e6d4c3ce94)); minItems=1; uniqueItems=true |  |
| <a id="s-5ead349b46"></a>`kernel` | no | [kernel](#s-2920056f4c) |  |
| <a id="s-a70310d6e2"></a>`name` | yes | [nonEmptyString](#s-ddcf450d4d) |  |
| <a id="s-01faf8abf9"></a>`version` | yes | [nonEmptyString](#s-ddcf450d4d) |  |

#### All must match (`allOf`)

| Rule | If schema matches | Then must match | Otherwise must match |
|---|---|---|---|
| <a id="s-86c79fea56"></a>1 | properties={family: (const="other")}; required=["family"] | required=["family_name"] | no additional constraint |

### <a id="s-a050110c08"></a>definition `organizationAgent`

- <a id="s-74174948f1"></a>`type`: `"object"`
- <a id="s-316cb4292c"></a>`additionalProperties`: `false`
- <a id="s-d853296f8a"></a>`required`: `["id","type"]`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-39fe9af531"></a>`id` | yes | [urnUuid](#s-b9f9301bba) |  |
| <a id="s-992df3c102"></a>`identifiers` | no | type="array"; items=([identifier](#s-e6d4c3ce94)); minItems=1; uniqueItems=true |  |
| <a id="s-4e25dad076"></a>`name` | no | [nonEmptyString](#s-ddcf450d4d) |  |
| <a id="s-683eae7db4"></a>`type` | yes | const="organization" |  |

### <a id="s-86c27faf36"></a>definition `payloadBinding`

- <a id="s-6791f4229b"></a>`type`: `"object"`
- <a id="s-840fd4da86"></a>`additionalProperties`: `false`
- <a id="s-d99afac1bc"></a>`description`: `"Append-only assertion binding a journal state to a payload locator relative to the sidecar. The latest effective event per role determines the current binding."`
- <a id="s-439f5ab588"></a>`required`: `["id","type","operation","role","asserted_by_agent_id"]`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-f6c309fe29"></a>`asserted_by_agent_id` | yes | [urnUuid](#s-b9f9301bba) |  |
| <a id="s-389eef412d"></a>`basis` | no | type="string"; enum=["size_and_sha256","repository_assertion","other"] |  |
| <a id="s-a34ff7ef23"></a>`basis_uri` | no | [absoluteUri](#s-088a6da7a6) |  |
| <a id="s-852514da24"></a>`established_by_activity_id` | no | [urnUuid](#s-b9f9301bba) |  |
| <a id="s-33a9620f12"></a>`established_by_capture_id` | no | [urnUuid](#s-b9f9301bba) |  |
| <a id="s-3c412cf1ff"></a>`id` | yes | [urnUuid](#s-b9f9301bba) |  |
| <a id="s-ec7a27b8c7"></a>`note` | no | [nonEmptyString](#s-ddcf450d4d) |  |
| <a id="s-ef90a56b83"></a>`operation` | yes | type="string"; enum=["bind","unbind"] |  |
| <a id="s-0ea8b9f5ed"></a>`relative_payload_locator` | no | [locator](#s-334ec6328c) |  |
| <a id="s-6ed3a8156b"></a>`replaces_binding_id` | no | [urnUuid](#s-b9f9301bba) |  |
| <a id="s-23e893b285"></a>`role` | yes | type="string"; enum=["co_resident_primary_payload","related_payload","other"] |  |
| <a id="s-8fde582ba3"></a>`role_uri` | no | [absoluteUri](#s-088a6da7a6) |  |
| <a id="s-43cbd1e6c4"></a>`state` | no | [stateReference](#s-6bc47c0dba) |  |
| <a id="s-18801733aa"></a>`type` | yes | const="payload_binding" |  |

#### All must match (`allOf`)

| Rule | If schema matches | Then must match | Otherwise must match |
|---|---|---|---|
| <a id="s-6cfeb542b2"></a>1 | properties={role: (const="other")}; required=["role"] | required=["role_uri"] | no additional constraint |
| <a id="s-23eceb927a"></a>2 | properties={operation: (const="bind")}; required=["operation"] | oneOf=(required=["established_by_capture_id"]) \| (required=["established_by_activity_id"]); required=["state","relative_payload_locator","basis"] | no additional constraint |
| <a id="s-ae5bd0d0ef"></a>3 | properties={operation: (const="unbind")}; required=["operation"] | not=(anyOf=(required=["state"]) \| (required=["relative_payload_locator"]) \| (required=["established_by_capture_id"]) \| (required=["established_by_activity_id"]) \| (required=["basis"]) \| (required=["basis_uri"])); required=["replaces_binding_id"] | no additional constraint |
| <a id="s-a76a027790"></a>4 | properties={basis: (const="other")}; required=["basis"] | required=["basis_uri"] | no additional constraint |

### <a id="s-5d421881fe"></a>definition `personAgent`

- <a id="s-4fa6cd376e"></a>`type`: `"object"`
- <a id="s-d0ec8f057f"></a>`additionalProperties`: `false`
- <a id="s-14167101cf"></a>`required`: `["id","type"]`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-a843adb0a5"></a>`id` | yes | [urnUuid](#s-b9f9301bba) |  |
| <a id="s-2183d6d8e7"></a>`identifiers` | no | type="array"; items=([identifier](#s-e6d4c3ce94)); minItems=1; uniqueItems=true |  |
| <a id="s-d14c15cfa4"></a>`name` | no | [nonEmptyString](#s-ddcf450d4d) |  |
| <a id="s-69b419383d"></a>`type` | yes | const="person" |  |

### <a id="s-94d0b65f81"></a>definition `portableString`

- <a id="s-a5bc928031"></a>`type`: `"string"`
- <a id="s-f0974de923"></a>`pattern`: `"^[^\\u0000\\uD800-\\uDFFF]*$"`

### <a id="s-dfb4db485f"></a>definition `principal`

- <a id="s-4c2b1fe289"></a>`type`: `"object"`
- <a id="s-fd5edaf2b7"></a>`additionalProperties`: `false`
- <a id="s-044e9a6c1d"></a>`required`: `["kind","resolution"]`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-64fe3643d1"></a>`identifiers` | no | type="array"; items=([identifier](#s-e6d4c3ce94)); minItems=1; uniqueItems=true |  |
| <a id="s-f3098efdcb"></a>`kind` | yes | type="string"; enum=["user","group","service","device","unknown"] |  |
| <a id="s-4c157ef834"></a>`name` | no | [nonEmptyString](#s-ddcf450d4d) |  |
| <a id="s-c966fc9859"></a>`resolution` | yes | type="string"; enum=["resolved","unresolved","not_attempted"] |  |

#### At least one must match (`anyOf`)

| Alternative | Schema |
|---|---|
| <a id="s-fcc01fcee7"></a>1 | required=["name"] |
| <a id="s-39d11e4b93"></a>2 | required=["identifiers"] |

### <a id="s-05bd4b5b33"></a>definition `processDetail`

- <a id="s-fe7d8529fc"></a>`type`: `"object"`
- <a id="s-50c8172eb2"></a>`additionalProperties`: `false`
- <a id="s-cca97ce9ad"></a>`minProperties`: `1`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-0469ac8a0a"></a>`command_line` | no | type="array"; items=([portableString](#s-94d0b65f81)); minItems=1 |  |
| <a id="s-1bb4b703a3"></a>`configuration_digest` | no | [digest](#s-1e1e5e54b0) |  |
| <a id="s-1ad0fbb219"></a>`external_event_identifier` | no | [identifier](#s-e6d4c3ce94) |  |
| <a id="s-9899729c6f"></a>`plan_id` | no | [absoluteUri](#s-088a6da7a6) |  |
| <a id="s-04aeb261ed"></a>`working_directory` | no | [locator](#s-334ec6328c) |  |

### <a id="s-9ccea05d10"></a>definition `processEvidence`

- <a id="s-7b81a4e4e9"></a>`type`: `"object"`
- <a id="s-66c4a7bf0e"></a>`additionalProperties`: `false`
- <a id="s-e3c3f07dcf"></a>`description`: `"Evidence supporting the asserted process type. Activity labels such as transcode are process provenance, never inferred from payload bytes unless explicitly marked as a comparison-based inference."`
- <a id="s-3b7babc0bf"></a>`required`: `["id","basis","asserted_by_agent_id","confidence"]`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-d2e5eb62a8"></a>`asserted_by_agent_id` | yes | [urnUuid](#s-b9f9301bba) |  |
| <a id="s-8f08e6d5e5"></a>`basis` | yes | type="string"; enum=["direct_process_record","application_log","operating_system_audit","repository_workflow","user_attestation","imported_provenance","inferred_from_state_comparison","unknown","other"] |  |
| <a id="s-2dfb135a89"></a>`basis_uri` | no | [absoluteUri](#s-088a6da7a6) |  |
| <a id="s-14e89f03de"></a>`comparison_relation_id` | no | [urnUuid](#s-b9f9301bba) |  |
| <a id="s-bc419b8f32"></a>`confidence` | yes | type="string"; enum=["high","medium","low","unknown"] |  |
| <a id="s-5581a22a67"></a>`description` | no | [nonEmptyString](#s-ddcf450d4d) |  |
| <a id="s-06dd944d2a"></a>`id` | yes | [urnUuid](#s-b9f9301bba) |  |
| <a id="s-4fb3fb1456"></a>`reference` | no | [typedValue](#s-de01b50b5c) |  |

#### All must match (`allOf`)

| Rule | If schema matches | Then must match | Otherwise must match |
|---|---|---|---|
| <a id="s-0edac3c287"></a>1 | properties={basis: (const="other")}; required=["basis"] | required=["basis_uri"] | no additional constraint |
| <a id="s-16a1d34d53"></a>2 | properties={basis: (const="inferred_from_state_comparison")}; required=["basis"] | required=["comparison_relation_id"] | no additional constraint |

### <a id="s-e81f29b9b5"></a>definition `provenanceContractReference`

- <a id="s-24e9a15561"></a>`type`: `"object"`
- <a id="s-066573ab41"></a>`additionalProperties`: `false`
- <a id="s-386a94f650"></a>`required`: `["format","provider","contract_id","contract_sha256"]`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-3364aa53c5"></a>`contract_id` | yes | [nonEmptyString](#s-ddcf450d4d) |  |
| <a id="s-fcb904cc59"></a>`contract_sha256` | yes | [sha256Hex](#s-1ceccb1b61) |  |
| <a id="s-55636c6819"></a>`format` | yes | const="riverhog-provenance-contract-reference/v1" |  |
| <a id="s-21e1491e3c"></a>`provider` | yes | [nonEmptyString](#s-ddcf450d4d) |  |

### <a id="s-307ea8eb10"></a>definition `provenanceObserverReference`

- <a id="s-8ccdc12357"></a>`type`: `"object"`
- <a id="s-9442eefdd1"></a>`additionalProperties`: `false`
- <a id="s-a91e891d99"></a>`dependentRequired`: `{"distribution":["version"],"version":["distribution"]}`
- <a id="s-67dd8542b6"></a>`required`: `["format","provider","observer_id","contract"]`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-18050facad"></a>`contract` | yes | [provenanceContractReference](#s-e81f29b9b5) |  |
| <a id="s-dda36f3f57"></a>`distribution` | no | [nonEmptyString](#s-ddcf450d4d) |  |
| <a id="s-cf1b267210"></a>`format` | yes | const="riverhog-provenance-observer-reference/v1" |  |
| <a id="s-c070509dc7"></a>`observer_id` | yes | [nonEmptyString](#s-ddcf450d4d) |  |
| <a id="s-852a4de7dc"></a>`provider` | yes | [nonEmptyString](#s-ddcf450d4d) |  |
| <a id="s-a566978066"></a>`version` | no | [nonEmptyString](#s-ddcf450d4d) |  |

### <a id="s-a65a100c0b"></a>definition `relation`


#### Exactly one must match (`oneOf`)

| Alternative | Schema |
|---|---|
| <a id="s-a918d66b5c"></a>1 | [usageRelation](#s-2a66a85deb) |
| <a id="s-894ba350f8"></a>2 | [generationRelation](#s-fd99a1f0e8) |
| <a id="s-436d92d225"></a>3 | [derivationRelation](#s-1d0efc506d) |
| <a id="s-906d2ab792"></a>4 | [invalidationRelation](#s-7be03ea942) |
| <a id="s-953e1b7491"></a>5 | [continuityRelation](#s-37e4bccab2) |
| <a id="s-c434fb0085"></a>6 | [comparisonRelation](#s-b92aeb48b4) |

### <a id="s-e8775f45df"></a>definition `relationRole`

- <a id="s-1366340f8d"></a>`type`: `"string"`
- <a id="s-00d49b7ada"></a>`enum`: `["source","input","reference","component","metadata_source","result","output","derivative","replacement","copy","other"]`

### <a id="s-a011de07f6"></a>definition `runtime`

- <a id="s-42f45433dd"></a>`type`: `"object"`
- <a id="s-005ac1ed00"></a>`additionalProperties`: `false`
- <a id="s-a5d3139010"></a>`required`: `["process_architecture","privilege"]`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-38120ec533"></a>`character_encoding` | no | [nonEmptyString](#s-ddcf450d4d) |  |
| <a id="s-1ce2a1a25e"></a>`container` | no | [nonEmptyString](#s-ddcf450d4d) |  |
| <a id="s-63f3d74ad5"></a>`effective_principal` | no | [principal](#s-dfb4db485f) |  |
| <a id="s-c712559922"></a>`identifiers` | no | type="array"; items=([identifier](#s-e6d4c3ce94)); minItems=1; uniqueItems=true |  |
| <a id="s-96501b61b5"></a>`locale` | no | [nonEmptyString](#s-ddcf450d4d) |  |
| <a id="s-23681415d0"></a>`privilege` | yes | type="string"; enum=["unprivileged","elevated","root","system","unknown"] |  |
| <a id="s-e6de7c4ffb"></a>`process_architecture` | yes | [nonEmptyString](#s-ddcf450d4d) |  |
| <a id="s-29af2778b9"></a>`time_zone` | no | [nonEmptyString](#s-ddcf450d4d) |  |
| <a id="s-f2874dc723"></a>`utc_offset` | no | type="string"; pattern="^[+-]&#40;?:[01][0-9]\|2[0-3]):[0-5][0-9]$" |  |

### <a id="s-2d2165b5a6"></a>definition `semanticAssertion`

- <a id="s-f4e540e33a"></a>`type`: `"object"`
- <a id="s-c4e721d4c5"></a>`additionalProperties`: `false`
- <a id="s-bdca93b57f"></a>`description`: `"URI-named semantic assertion over the closed typed-value union. It cannot redefine core Riverhog provenance semantics."`
- <a id="s-7045f4db3b"></a>`required`: `["id","type","subject_id","property","value","asserted_by_agent_id","confidence"]`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-3d72d08991"></a>`asserted_by_agent_id` | yes | [urnUuid](#s-b9f9301bba) |  |
| <a id="s-42cab5e8d1"></a>`confidence` | yes | type="string"; enum=["high","medium","low","unknown"] |  |
| <a id="s-901da705b2"></a>`id` | yes | [urnUuid](#s-b9f9301bba) |  |
| <a id="s-0c7bd66694"></a>`note` | no | [nonEmptyString](#s-ddcf450d4d) |  |
| <a id="s-fcdaf86523"></a>`property` | yes | [absoluteUri](#s-088a6da7a6) |  |
| <a id="s-5963991a65"></a>`subject_id` | yes | [absoluteUri](#s-088a6da7a6) |  |
| <a id="s-190196f926"></a>`type` | yes | const="semantic_assertion" |  |
| <a id="s-34e06a66f9"></a>`value` | yes | [typedValue](#s-de01b50b5c) |  |

### <a id="s-1ceccb1b61"></a>definition `sha256Hex`

- <a id="s-df01b1d0bb"></a>`type`: `"string"`
- <a id="s-7b29c1e01b"></a>`pattern`: `"^[0-9a-f]{64}$"`

### <a id="s-e4f49d8758"></a>definition `softwareAgent`

- <a id="s-6adf1edb22"></a>`type`: `"object"`
- <a id="s-b7cee01510"></a>`additionalProperties`: `false`
- <a id="s-4a5ee556a9"></a>`required`: `["id","type","name"]`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-206b598b49"></a>`build` | no | [nonEmptyString](#s-ddcf450d4d) |  |
| <a id="s-a13bbfca55"></a>`executable_digests` | no | type="array"; items=([digest](#s-1e1e5e54b0)); minItems=1; uniqueItems=true |  |
| <a id="s-54f59d8778"></a>`id` | yes | [urnUuid](#s-b9f9301bba) |  |
| <a id="s-0524e1fcb7"></a>`identifiers` | no | type="array"; items=([identifier](#s-e6d4c3ce94)); minItems=1; uniqueItems=true |  |
| <a id="s-7b6e4759d5"></a>`name` | yes | [nonEmptyString](#s-ddcf450d4d) |  |
| <a id="s-26127b6e69"></a>`type` | yes | const="software" |  |
| <a id="s-639a9656b9"></a>`vendor` | no | [nonEmptyString](#s-ddcf450d4d) |  |
| <a id="s-e58deaaf06"></a>`version` | no | [nonEmptyString](#s-ddcf450d4d) |  |

#### At least one must match (`anyOf`)

| Alternative | Schema |
|---|---|
| <a id="s-7ad2027d0f"></a>1 | required=["version"] |
| <a id="s-3ed3dc5dbe"></a>2 | required=["build"] |
| <a id="s-67aed847a4"></a>3 | required=["executable_digests"] |

### <a id="s-6bcbe0dfe1"></a>definition `sourceDescriptor`

- <a id="s-6dee3b2ed1"></a>`type`: `"object"`
- <a id="s-79a3e2c891"></a>`additionalProperties`: `false`
- <a id="s-a978509b0e"></a>`required`: `["platform","api"]`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-d112a57d19"></a>`api` | yes | [nonEmptyString](#s-ddcf450d4d) |  |
| <a id="s-78ba3134ab"></a>`api_uri` | no | [absoluteUri](#s-088a6da7a6) |  |
| <a id="s-d47493ef5b"></a>`field` | no | [nonEmptyString](#s-ddcf450d4d) |  |
| <a id="s-1d8fefbe94"></a>`field_uri` | no | [absoluteUri](#s-088a6da7a6) |  |
| <a id="s-429ead5be6"></a>`platform` | yes | [sourcePlatform](#s-e5152341be) |  |
| <a id="s-b4d3f56714"></a>`platform_name` | no | [nonEmptyString](#s-ddcf450d4d) |  |
| <a id="s-1439cd582a"></a>`version` | no | [nonEmptyString](#s-ddcf450d4d) |  |

#### All must match (`allOf`)

| Rule | If schema matches | Then must match | Otherwise must match |
|---|---|---|---|
| <a id="s-63d960fce8"></a>1 | properties={platform: (const="other")}; required=["platform"] | required=["platform_name"] | no additional constraint |

### <a id="s-e5152341be"></a>definition `sourcePlatform`

- <a id="s-da1117be0e"></a>`type`: `"string"`
- <a id="s-aae155ae34"></a>`enum`: `["linux","windows","macos","freebsd","openbsd","netbsd","illumos","aix","posix","android","ios","solaris","other"]`

### <a id="s-6bc47c0dba"></a>definition `stateReference`

- <a id="s-207654ce5c"></a>`type`: `"object"`
- <a id="s-7c0d2c49f9"></a>`additionalProperties`: `false`
- <a id="s-fe2f14875b"></a>`dependentRequired`: `{"entry_id":["entry_json_sha256"],"entry_json_sha256":["entry_id"]}`
- <a id="s-98494c9dd8"></a>`required`: `["id","scope"]`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-d1c0d31895"></a>`entry_id` | no | [urnUuid](#s-b9f9301bba) |  |
| <a id="s-f55aa3e537"></a>`entry_json_sha256` | no | [sha256Hex](#s-1ceccb1b61) |  |
| <a id="s-f1b62f443d"></a>`id` | yes | [urnUuid](#s-b9f9301bba) |  |
| <a id="s-4a99058d34"></a>`journal_id` | no | [urnUuid](#s-b9f9301bba) |  |
| <a id="s-6909a9c6e3"></a>`scope` | yes | type="string"; enum=["local","external"] |  |
| <a id="s-a1c208c91f"></a>`sidecar_uri` | no | [absoluteUri](#s-088a6da7a6) |  |

#### All must match (`allOf`)

| Rule | If schema matches | Then must match | Otherwise must match |
|---|---|---|---|
| <a id="s-77203dbb83"></a>1 | properties={scope: (const="local")}; required=["scope"] | not=(anyOf=(required=["journal_id"]) \| (required=["entry_id"]) \| (required=["entry_json_sha256"]) \| (required=["sidecar_uri"])) | no additional constraint |
| <a id="s-fc57b64bd7"></a>2 | properties={scope: (const="external")}; required=["scope"] | required=["journal_id"] | no additional constraint |

### <a id="s-ce6e164ba2"></a>definition `textValue`

- <a id="s-f5500cd1d0"></a>`type`: `"object"`
- <a id="s-285f448f19"></a>`additionalProperties`: `false`
- <a id="s-c7b9cd00f1"></a>`dependentRequired`: `{"byte_length":["source_encoding"]}`
- <a id="s-daedf20a9e"></a>`required`: `["type","data"]`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-8748aa219f"></a>`byte_length` | no | type="integer"; minimum=0; maximum=9223372036854775807 |  |
| <a id="s-45c3bb6a66"></a>`data` | yes | [portableString](#s-94d0b65f81) |  |
| <a id="s-c00970494c"></a>`language` | no | type="string"; pattern="^[A-Za-z]{2,8}(?:-[A-Za-z0-9]{1,8})*$" |  |
| <a id="s-8a58123f1d"></a>`media_type` | no | [nonEmptyString](#s-ddcf450d4d) |  |
| <a id="s-a015711de8"></a>`source_encoding` | no | [nonEmptyString](#s-ddcf450d4d) |  |
| <a id="s-dfcf929af8"></a>`type` | yes | const="text" |  |

### <a id="s-f1f3653cef"></a>definition `timestampObservation`

- <a id="s-6eab3050a9"></a>`type`: `"object"`
- <a id="s-e43290752b"></a>`additionalProperties`: `false`
- <a id="s-d07fa8767e"></a>`dependentRequired`: `{"raw_epoch":["raw_value"],"raw_unit":["raw_value"]}`
- <a id="s-b893cbb918"></a>`required`: `["kind","value_status","source"]`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-662e9b512f"></a>`assumption` | no | [nonEmptyString](#s-ddcf450d4d) |  |
| <a id="s-8e7c70778a"></a>`kind` | yes | type="string"; enum=["created","content_modified","metadata_changed","accessed","backup","archived","other"] |  |
| <a id="s-87bd16e55b"></a>`kind_uri` | no | [absoluteUri](#s-088a6da7a6) |  |
| <a id="s-0c937d8ca1"></a>`raw_epoch` | no | [utcDateTime](#s-57f05b370e) |  |
| <a id="s-dba7da7015"></a>`raw_unit` | no | type="string"; enum=["seconds","milliseconds","microseconds","nanoseconds","ticks_100ns","days","other"] |  |
| <a id="s-77cd9ff22d"></a>`raw_unit_name` | no | [nonEmptyString](#s-ddcf450d4d) |  |
| <a id="s-fe863721b6"></a>`raw_value` | no | [nonEmptyString](#s-ddcf450d4d) |  |
| <a id="s-f82ef2c615"></a>`resolution_ns` | no | type="integer"; minimum=1; maximum=9223372036854775807 |  |
| <a id="s-68c0817af3"></a>`source` | yes | [fieldSourceDescriptor](#s-dd3ca68abf) |  |
| <a id="s-c4bdf83334"></a>`value` | no | [utcDateTime](#s-57f05b370e) |  |
| <a id="s-1ca1a2cf47"></a>`value_status` | yes | type="string"; enum=["exact","assumed","unresolved"] |  |

#### All must match (`allOf`)

| Rule | If schema matches | Then must match | Otherwise must match |
|---|---|---|---|
| <a id="s-7fafead695"></a>1 | properties={kind: (const="other")}; required=["kind"] | required=["kind_uri"] | no additional constraint |
| <a id="s-ba518b7121"></a>2 | properties={value_status: (enum=["exact","assumed"])}; required=["value_status"] | required=["value","resolution_ns"] | no additional constraint |
| <a id="s-198087ea3b"></a>3 | properties={value_status: (const="assumed")}; required=["value_status"] | required=["assumption"] | no additional constraint |
| <a id="s-b7b31a786c"></a>4 | properties={value_status: (const="unresolved")}; required=["value_status"] | not=(required=["value"]); required=["raw_value"] | no additional constraint |
| <a id="s-99832129f9"></a>5 | properties={raw_unit: (const="other")}; required=["raw_unit"] | required=["raw_unit_name"] | no additional constraint |

### <a id="s-9d92b09f3e"></a>definition `timestampValue`

- <a id="s-35c959e048"></a>`type`: `"object"`
- <a id="s-5aa7225bc6"></a>`additionalProperties`: `false`
- <a id="s-963d9005c9"></a>`required`: `["type","data"]`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-49986e656e"></a>`data` | yes | [utcDateTime](#s-57f05b370e) |  |
| <a id="s-d37b49c394"></a>`resolution_ns` | no | type="integer"; minimum=1; maximum=9223372036854775807 |  |
| <a id="s-d04569613e"></a>`type` | yes | const="timestamp" |  |

### <a id="s-95e37398de"></a>definition `transitionActivity`

- <a id="s-e2be5e48e3"></a>`type`: `"object"`
- <a id="s-018d753a0c"></a>`additionalProperties`: `false`
- <a id="s-1484267282"></a>`description`: `"A real-world Activity/PREMIS Event that used, generated, or invalidated file states. It is distinct from observation capture."`
- <a id="s-c92a5f697e"></a>`required`: `["id","type","event_type","time","outcome","evidence"]`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-982d0bba39"></a>`associations` | no | type="array"; items=([association](#s-bdc5771284)); minItems=1; uniqueItems=true |  |
| <a id="s-ec90017218"></a>`detail` | no | [processDetail](#s-05bd4b5b33) |  |
| <a id="s-3678fa9f63"></a>`environment_id` | no | [urnUuid](#s-b9f9301bba) |  |
| <a id="s-bb7128e69c"></a>`event_label` | no | [nonEmptyString](#s-ddcf450d4d) |  |
| <a id="s-6f61057fb8"></a>`event_type` | yes | type="string"; enum=["creation","content_modification","metadata_update","transformation","copy","relocation","deletion","composite","unknown_change","other"] |  |
| <a id="s-9a58963e68"></a>`event_type_uri` | no | [absoluteUri](#s-088a6da7a6) |  |
| <a id="s-e93307925c"></a>`evidence` | yes | type="array"; items=([processEvidence](#s-9ccea05d10)); minItems=1; uniqueItems=true |  |
| <a id="s-c862c97c41"></a>`id` | yes | [urnUuid](#s-b9f9301bba) |  |
| <a id="s-3606543f5e"></a>`notes` | no | type="array"; items=([nonEmptyString](#s-ddcf450d4d)); minItems=1; uniqueItems=true |  |
| <a id="s-8f70a83796"></a>`outcome` | yes | type="string"; enum=["success","partial","failure","unknown"] |  |
| <a id="s-fa594482f4"></a>`time` | yes | [activityTime](#s-afa0679572) |  |
| <a id="s-4ae1293663"></a>`type` | yes | const="file_state_transition" |  |

#### All must match (`allOf`)

| Rule | If schema matches | Then must match | Otherwise must match |
|---|---|---|---|
| <a id="s-cd1df6360a"></a>1 | properties={event_type: (const="other")}; required=["event_type"] | required=["event_type_uri"] | no additional constraint |

### <a id="s-de01b50b5c"></a>definition `typedValue`

- <a id="s-2e568347e5"></a>`description`: `"Closed tagged value union."`

#### Exactly one must match (`oneOf`)

| Alternative | Schema |
|---|---|
| <a id="s-b08d331f18"></a>1 | [capturedValue](#s-14ff4aeeaf) |
| <a id="s-9ecf7193ee"></a>2 | [digestOnlyValue](#s-b87a395047) |

### <a id="s-6dc8938549"></a>definition `uriValue`

- <a id="s-5302b7b72d"></a>`type`: `"object"`
- <a id="s-5e187241d4"></a>`additionalProperties`: `false`
- <a id="s-14d51478ef"></a>`required`: `["type","data"]`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-25580f0b4d"></a>`data` | yes | [absoluteUri](#s-088a6da7a6) |  |
| <a id="s-72aabbd2d2"></a>`type` | yes | const="uri" |  |

### <a id="s-b9f9301bba"></a>definition `urnUuid`

- <a id="s-61426b4d62"></a>`type`: `"string"`
- <a id="s-7f2b6ebe49"></a>`pattern`: `"^urn:uuid:[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$"`

### <a id="s-2a66a85deb"></a>definition `usageRelation`

- <a id="s-15a50ff874"></a>`type`: `"object"`
- <a id="s-257b0dbb04"></a>`additionalProperties`: `false`
- <a id="s-d058db9ef5"></a>`required`: `["id","type","activity_id","state","role"]`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-bb5b11a48f"></a>`activity_id` | yes | [urnUuid](#s-b9f9301bba) |  |
| <a id="s-2ff2f57fcb"></a>`id` | yes | [urnUuid](#s-b9f9301bba) |  |
| <a id="s-a08a21360f"></a>`role` | yes | [relationRole](#s-e8775f45df) |  |
| <a id="s-34a1450c12"></a>`role_uri` | no | [absoluteUri](#s-088a6da7a6) |  |
| <a id="s-abcca51bab"></a>`state` | yes | [stateReference](#s-6bc47c0dba) |  |
| <a id="s-c90fbb9421"></a>`type` | yes | const="usage" |  |

#### All must match (`allOf`)

| Rule | If schema matches | Then must match | Otherwise must match |
|---|---|---|---|
| <a id="s-96d05673f8"></a>1 | properties={role: (const="other")}; required=["role"] | required=["role_uri"] | no additional constraint |

### <a id="s-57f05b370e"></a>definition `utcDateTime`

- <a id="s-b1ebb04e37"></a>`type`: `"string"`
- <a id="s-b76bea7a9a"></a>`format`: `"date-time"`
- <a id="s-59006cc684"></a>`description`: `"RFC 3339 date-time normalized to UTC with uppercase Z and at most nanosecond precision."`
- <a id="s-ea6f434dcb"></a>`pattern`: `"^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(?:\\.[0-9]{1,9})?Z$"`

### <a id="s-42651f0d7d"></a>definition `captureEvent` · field `associations`

- <a id="s-806c7ac6ed"></a>`type`: `"array"`
- `contains`: [See definition `captureEvent` · field `associations` · `contains`](#s-3b21c7e910)
- <a id="s-4c02493aa2"></a>`items`: [association](#s-bdc5771284)
- <a id="s-b9f4c11924"></a>`minContains`: `1`
- <a id="s-77fe113bcd"></a>`minItems`: `1`
- <a id="s-3365121966"></a>`uniqueItems`: `true`

### <a id="s-e61e291f8a"></a>definition `captureEvent` · field `operations`

- <a id="s-7b58287675"></a>`type`: `"array"`
- <a id="s-c62864a553"></a>`items`: type="string"; enum=["metadata_extraction","message_digest_calculation","principal_resolution"]
- <a id="s-8550946956"></a>`minItems`: `2`
- <a id="s-c02660d4c1"></a>`uniqueItems`: `true`

#### All must match (`allOf`)

| Alternative | Schema |
|---|---|
| <a id="s-cfaa94d8f1"></a>1 | contains=(const="metadata_extraction"); minContains=1 |
| <a id="s-452d3bfbbe"></a>2 | contains=(const="message_digest_calculation"); minContains=1 |

### <a id="s-d8a3c25d9d"></a>definition `contentDescription` · field `digests`

- <a id="s-fb3dfe4b2d"></a>`type`: `"array"`
- `contains`: [See definition `contentDescription` · field `digests` · `contains`](#s-ab616a6db9)
- <a id="s-cdd55f9147"></a>`items`: [digest](#s-1e1e5e54b0)
- <a id="s-8262872f84"></a>`maxContains`: `1`
- <a id="s-4889339b49"></a>`minContains`: `1`
- <a id="s-fd3cbf3b9d"></a>`minItems`: `1`
- <a id="s-ac6961f459"></a>`uniqueItems`: `true`

### <a id="s-3b21c7e910"></a>definition `captureEvent` · field `associations` · `contains`

- <a id="s-22214740e3"></a>`type`: `"object"`
- <a id="s-dab45881bf"></a>`required`: `["role"]`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-af03677d61"></a>`role` | yes | const="executing_software" |  |

### <a id="s-ab616a6db9"></a>definition `contentDescription` · field `digests` · `contains`

- <a id="s-741073d169"></a>`type`: `"object"`
- <a id="s-90f0c65668"></a>`required`: `["algorithm","purpose"]`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-47d560b535"></a>`algorithm` | yes | const="sha-256" |  |
| <a id="s-5fe1425673"></a>`purpose` | yes | const="fixity" |  |

### Progression, limits, and lifecycle

#### [extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48a)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"https://nashspence.github.io/riverhog/v1/provenance/journal-entry.schema.json"}; maximum=null; reason="no-declared-semantic-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [definition accessMetadata](#s-f7179075a7) | `cardinality · entries · operational_policy` | shared above |
| [definition byteString · field digests](#s-ebc2a4d88d) | `cardinality · items · operational_policy` | shared above |
| [definition bytesValue · field digests](#s-4426948ec3) | `cardinality · items · operational_policy` | shared above |
| [definition captureDetail · field command_line](#s-6352076890) | `cardinality · items · operational_policy` | shared above |
| [definition captureDetail](#s-f1bbe8d25c) | `cardinality · entries · operational_policy` | shared above |
| [definition captureEvent · field associations](#s-42651f0d7d) | `cardinality · items · operational_policy` | shared above |
| [definition captureEvent · field diagnostics](#s-e1676a2787) | `cardinality · items · operational_policy` | shared above |
| [definition captureEvent · field notes](#s-9026ae9f30) | `cardinality · items · operational_policy` | shared above |
| [definition captureEvent · field operations](#s-e61e291f8a) | `cardinality · items · operational_policy` | shared above |
| [definition comparisonDimension · field basis](#s-f7e5cae39a) | `cardinality · items · operational_policy` | shared above |
| [definition comparisonRelation · field dimensions](#s-089c1457ee) | `cardinality · items · operational_policy` | shared above |
| [definition comparisonRelation · field notes](#s-7af1f6b6a5) | `cardinality · items · operational_policy` | shared above |
| [definition contentDescription · field digests](#s-d8a3c25d9d) | `cardinality · items · operational_policy` | shared above |
| [definition continuityRelation · field basis](#s-1396b618b3) | `cardinality · items · operational_policy` | shared above |
| [definition correctionBody · field supersedes](#s-49a4432a2c) | `cardinality · items · operational_policy` | shared above |
| [definition digestOnlyValue · field digests](#s-bf0abcdad0) | `cardinality · items · operational_policy` | shared above |
| [definition fileLineage · field identifiers](#s-0d3b23ad9d) | `cardinality · items · operational_policy` | shared above |
| [definition fileLineage · field notes](#s-7ea2d17ae9) | `cardinality · items · operational_policy` | shared above |
| [definition fileState · field notes](#s-8a8b9a8e50) | `cardinality · items · operational_policy` | shared above |
| [definition filesystem · field snapshot_identifiers](#s-3fa7d05459) | `cardinality · items · operational_policy` | shared above |
| [definition filesystem · field volume_identifiers](#s-93f4226725) | `cardinality · items · operational_policy` | shared above |
| [definition filesystemMetadata · field native_identifiers](#s-85b269c7a3) | `cardinality · items · operational_policy` | shared above |
| [definition filesystemMetadata · field native_metadata](#s-6957b5d6cb) | `cardinality · items · operational_policy` | shared above |
| [definition filesystemMetadata · field timestamps](#s-10ce445ffb) | `cardinality · items · operational_policy` | shared above |
| [definition graphFragment · field activities](#s-4bc7c3210b) | `cardinality · items · operational_policy` | shared above |
| [definition graphFragment · field agents](#s-f8492f18ad) | `cardinality · items · operational_policy` | shared above |
| [definition graphFragment · field captures](#s-08b1a0dcef) | `cardinality · items · operational_policy` | shared above |
| [definition graphFragment · field environments](#s-3fdd6cd86d) | `cardinality · items · operational_policy` | shared above |
| [definition graphFragment · field extensions](#s-c242ca70cf) | `cardinality · items · operational_policy` | shared above |
| [definition graphFragment · field lineages](#s-951061ad06) | `cardinality · items · operational_policy` | shared above |
| [definition graphFragment · field payload_bindings](#s-f7fd60d745) | `cardinality · items · operational_policy` | shared above |
| [definition graphFragment · field relations](#s-02ccd37f77) | `cardinality · items · operational_policy` | shared above |
| [definition graphFragment · field states](#s-dc1fadd69b) | `cardinality · items · operational_policy` | shared above |
| [definition graphFragment](#s-b3f23dc219) | `cardinality · entries · operational_policy` | shared above |
| [definition hardwareAgent · field identifiers](#s-08e920634a) | `cardinality · items · operational_policy` | shared above |
| [definition host · field identifiers](#s-f69daf1102) | `cardinality · items · operational_policy` | shared above |
| [definition kernel](#s-2920056f4c) | `cardinality · entries · operational_policy` | shared above |
| [definition nativeMetadata · field interpretations](#s-e99672219a) | `cardinality · items · operational_policy` | shared above |
| [definition nonNullJson · array value](#s-4bfccb267e) | `cardinality · items · operational_policy` | shared above |
| [definition nonNullJson · object value](#s-b8d3cf55bd) | `cardinality · entries · operational_policy` | shared above |
| [definition operatingSystem · field identifiers](#s-9b913d3cb1) | `cardinality · items · operational_policy` | shared above |
| [definition organizationAgent · field identifiers](#s-992df3c102) | `cardinality · items · operational_policy` | shared above |
| [definition personAgent · field identifiers](#s-2183d6d8e7) | `cardinality · items · operational_policy` | shared above |
| [definition principal · field identifiers](#s-64fe3643d1) | `cardinality · items · operational_policy` | shared above |
| [definition processDetail · field command_line](#s-0469ac8a0a) | `cardinality · items · operational_policy` | shared above |
| [definition processDetail](#s-05bd4b5b33) | `cardinality · entries · operational_policy` | shared above |
| [definition runtime · field identifiers](#s-c712559922) | `cardinality · items · operational_policy` | shared above |
| [definition softwareAgent · field executable_digests](#s-a13bbfca55) | `cardinality · items · operational_policy` | shared above |
| [definition softwareAgent · field identifiers](#s-0524e1fcb7) | `cardinality · items · operational_policy` | shared above |
| [definition transitionActivity · field associations](#s-982d0bba39) | `cardinality · items · operational_policy` | shared above |
| [definition transitionActivity · field evidence](#s-e93307925c) | `cardinality · items · operational_policy` | shared above |
| [definition transitionActivity · field notes](#s-3606543f5e) | `cardinality · items · operational_policy` | shared above |
| [field notes](#s-0bcb488eba) | `cardinality · items · operational_policy` | shared above |

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [definition accessMetadata · field posix_mode](#s-3d49292458) | `length · characters · fixed` | maximum=4; minimum=4; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-7]{4}$"} |
| [definition byteString · field byte_length](#s-20d5f4d688) | `value · schema-value · contract_max` | maximum=9223372036854775807; minimum=0; reason="schema-maximum" |
| [definition bytesValue · field byte_length](#s-d7fe9175c0) | `value · schema-value · contract_max` | maximum=9223372036854775807; minimum=0; reason="schema-maximum" |
| [definition checkpointCounts · field activities](#s-92b955b86b) | `value · schema-value · contract_max` | maximum=9223372036854775807; minimum=0; reason="schema-maximum" |
| [definition checkpointCounts · field captures](#s-fd70682b59) | `value · schema-value · contract_max` | maximum=9223372036854775807; minimum=0; reason="schema-maximum" |
| [definition checkpointCounts · field entries](#s-c697031463) | `value · schema-value · contract_max` | maximum=9223372036854775807; minimum=1; reason="schema-maximum" |
| [definition checkpointCounts · field lineages](#s-c3f33d3b72) | `value · schema-value · contract_max` | maximum=9223372036854775807; minimum=0; reason="schema-maximum" |
| [definition checkpointCounts · field relations](#s-e41cbc72e4) | `value · schema-value · contract_max` | maximum=9223372036854775807; minimum=0; reason="schema-maximum" |
| [definition checkpointCounts · field states](#s-917de38a38) | `value · schema-value · contract_max` | maximum=9223372036854775807; minimum=0; reason="schema-maximum" |
| [definition contentDescription · field size_bytes](#s-26c2b67719) | `value · schema-value · contract_max` | maximum=9223372036854775807; minimum=0; reason="schema-maximum" |
| [definition diagnostic · field code](#s-aca6a83557) | `length · characters · contract_max` | maximum=255; minimum=1; reason="schema-maximum" |
| <a id="s-1ab9b3b9fb"></a>[definition digest · allOf alternative 1 · then · field value](#s-993f370b79) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| <a id="s-0cc851c7dd"></a>[definition digest · allOf alternative 2 · then · field value](#s-765441b45e) | `length · characters · fixed` | maximum=128; minimum=128; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{128}$"} |
| [definition digestOnlyValue · field byte_length](#s-c7446adbb4) | `value · schema-value · contract_max` | maximum=9223372036854775807; minimum=0; reason="schema-maximum" |
| [definition entryReference · field sequence](#s-f4eef91776) | `value · schema-value · contract_max` | maximum=9223372036854775807; minimum=0; reason="schema-maximum" |
| [definition filesystem · field type](#s-9f1b983483) | `length · characters · contract_max` | maximum=128; minimum=1; reason="schema-maximum" |
| <a id="s-59c007d25a"></a>[definition identifier · allOf alternative 1 · then · field value](#s-86b21d23fb) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [definition identifier · field scheme](#s-7b8bf71694) | `length · characters · contract_max` | maximum=255; minimum=1; reason="schema-maximum" |
| [definition nativeMetadata · field observed_byte_length](#s-b98cbebd2e) | `value · schema-value · contract_max` | maximum=9223372036854775807; minimum=0; reason="schema-maximum" |
| <a id="s-11be07c333"></a>[definition observedIdentifier · allOf alternative 1 · then · field value](#s-5ed40337a8) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [definition observedIdentifier · field scheme](#s-7ffa2e848b) | `length · characters · contract_max` | maximum=255; minimum=1; reason="schema-maximum" |
| [definition sha256Hex](#s-1ceccb1b61) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [definition textValue · field byte_length](#s-8748aa219f) | `value · schema-value · contract_max` | maximum=9223372036854775807; minimum=0; reason="schema-maximum" |
| [definition timestampObservation · field resolution_ns](#s-f82ef2c615) | `value · schema-value · contract_max` | maximum=9223372036854775807; minimum=1; reason="schema-maximum" |
| [definition timestampValue · field resolution_ns](#s-d37b49c394) | `value · schema-value · contract_max` | maximum=9223372036854775807; minimum=1; reason="schema-maximum" |
| [field sequence](#s-a755e6074e) | `value · schema-value · contract_max` | maximum=9223372036854775807; minimum=0; reason="schema-maximum" |

## Governing policies

- <a id="pa-c746152eaa"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)
- <a id="pa-b3e2a1266e"></a>[extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48a)
- <a id="pa-7534c844ee"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [protocol:https://nashspence.github.io/riverhog/v1/provenance/journal-entry.schema.json](../../../evidence/sources.md#src-819610ba95) — `packages/riverhog-provenance/src/riverhog_provenance/schemas/riverhog-provenance-v1-journal-entry.schema.json`

### Machine authority

- `/external_contract/protocol_schemas/https:~1~1nashspence.github.io~1riverhog~1v1~1provenance~1journal-entry.schema.json`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

Large integers appear as decimal strings in this machine representation. The machine artifact's `projection_unsafe_integer_paths` identifies them; primary content displays the recovered numeric values.

<!-- exact-contract-value: 5b93f8c2bb1707ac9179e764afc163ed06036bf508fb7e9692dfb9930602e738 -->

```json
{
  "$defs": {
    "absoluteUri": {
      "format": "uri",
      "pattern": "^[^\\u0000\\uD800-\\uDFFF]+$",
      "type": "string"
    },
    "accessMetadata": {
      "additionalProperties": false,
      "minProperties": 1,
      "properties": {
        "group": {
          "$ref": "#/$defs/principal"
        },
        "owner": {
          "$ref": "#/$defs/principal"
        },
        "posix_mode": {
          "pattern": "^[0-7]{4}$",
          "type": "string"
        }
      },
      "type": "object"
    },
    "activityTime": {
      "additionalProperties": false,
      "allOf": [
        {
          "if": {
            "properties": {
              "status": {
                "const": "exact"
              }
            },
            "required": [
              "status"
            ]
          },
          "then": {
            "required": [
              "started_at",
              "ended_at"
            ]
          }
        },
        {
          "if": {
            "properties": {
              "status": {
                "const": "approximate"
              }
            },
            "required": [
              "status"
            ]
          },
          "then": {
            "anyOf": [
              {
                "required": [
                  "started_at"
                ]
              },
              {
                "required": [
                  "ended_at"
                ]
              }
            ],
            "required": [
              "note"
            ]
          }
        },
        {
          "if": {
            "properties": {
              "status": {
                "const": "bounded"
              }
            },
            "required": [
              "status"
            ]
          },
          "then": {
            "required": [
              "started_at",
              "ended_at",
              "note"
            ]
          }
        },
        {
          "if": {
            "properties": {
              "status": {
                "const": "unknown"
              }
            },
            "required": [
              "status"
            ]
          },
          "then": {
            "not": {
              "anyOf": [
                {
                  "required": [
                    "started_at"
                  ]
                },
                {
                  "required": [
                    "ended_at"
                  ]
                }
              ]
            },
            "required": [
              "note"
            ]
          }
        }
      ],
      "properties": {
        "ended_at": {
          "$ref": "#/$defs/utcDateTime"
        },
        "note": {
          "$ref": "#/$defs/nonEmptyString"
        },
        "started_at": {
          "$ref": "#/$defs/utcDateTime"
        },
        "status": {
          "enum": [
            "exact",
            "approximate",
            "bounded",
            "unknown"
          ],
          "type": "string"
        }
      },
      "required": [
        "status"
      ],
      "type": "object"
    },
    "agent": {
      "oneOf": [
        {
          "$ref": "#/$defs/softwareAgent"
        },
        {
          "$ref": "#/$defs/personAgent"
        },
        {
          "$ref": "#/$defs/organizationAgent"
        },
        {
          "$ref": "#/$defs/hardwareAgent"
        }
      ]
    },
    "assertionBody": {
      "additionalProperties": false,
      "properties": {
        "assertions": {
          "$ref": "#/$defs/graphFragment"
        }
      },
      "required": [
        "assertions"
      ],
      "type": "object"
    },
    "association": {
      "additionalProperties": false,
      "allOf": [
        {
          "if": {
            "properties": {
              "role": {
                "const": "other"
              }
            },
            "required": [
              "role"
            ]
          },
          "then": {
            "required": [
              "role_uri"
            ]
          }
        },
        {
          "if": {
            "properties": {
              "role": {
                "const": "executing_software"
              }
            },
            "required": [
              "role"
            ]
          },
          "then": {
            "required": [
              "plan_id"
            ]
          }
        }
      ],
      "properties": {
        "agent_id": {
          "$ref": "#/$defs/urnUuid"
        },
        "plan_id": {
          "$ref": "#/$defs/absoluteUri"
        },
        "role": {
          "enum": [
            "executing_software",
            "operator",
            "responsible_organization",
            "authorizing_agent",
            "capture_device",
            "process_owner",
            "creator",
            "editor",
            "reviewer",
            "other"
          ],
          "type": "string"
        },
        "role_uri": {
          "$ref": "#/$defs/absoluteUri"
        }
      },
      "required": [
        "agent_id",
        "role"
      ],
      "type": "object"
    },
    "booleanValue": {
      "additionalProperties": false,
      "properties": {
        "data": {
          "type": "boolean"
        },
        "type": {
          "const": "boolean"
        }
      },
      "required": [
        "type",
        "data"
      ],
      "type": "object"
    },
    "byteString": {
      "additionalProperties": false,
      "properties": {
        "byte_length": {
          "maximum": "9223372036854775807",
          "minimum": 0,
          "type": "integer"
        },
        "data": {
          "pattern": "^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?$",
          "type": "string"
        },
        "digests": {
          "items": {
            "$ref": "#/$defs/digest"
          },
          "minItems": 1,
          "type": "array",
          "uniqueItems": true
        },
        "encoding": {
          "const": "base64"
        },
        "media_type": {
          "$ref": "#/$defs/nonEmptyString"
        }
      },
      "required": [
        "encoding",
        "data",
        "byte_length"
      ],
      "type": "object"
    },
    "bytesValue": {
      "additionalProperties": false,
      "properties": {
        "byte_length": {
          "maximum": "9223372036854775807",
          "minimum": 0,
          "type": "integer"
        },
        "data": {
          "pattern": "^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?$",
          "type": "string"
        },
        "digests": {
          "items": {
            "$ref": "#/$defs/digest"
          },
          "minItems": 1,
          "type": "array",
          "uniqueItems": true
        },
        "encoding": {
          "const": "base64"
        },
        "media_type": {
          "$ref": "#/$defs/nonEmptyString"
        },
        "type": {
          "const": "bytes"
        }
      },
      "required": [
        "type",
        "encoding",
        "data",
        "byte_length"
      ],
      "type": "object"
    },
    "captureDetail": {
      "additionalProperties": false,
      "minProperties": 1,
      "properties": {
        "command_line": {
          "items": {
            "$ref": "#/$defs/portableString"
          },
          "minItems": 1,
          "type": "array"
        },
        "configuration_digest": {
          "$ref": "#/$defs/digest"
        },
        "profile_id": {
          "$ref": "#/$defs/absoluteUri"
        },
        "provenance_observer": {
          "$ref": "#/$defs/provenanceObserverReference"
        },
        "working_directory": {
          "$ref": "#/$defs/locator"
        }
      },
      "type": "object"
    },
    "captureEvent": {
      "additionalProperties": false,
      "allOf": [
        {
          "if": {
            "properties": {
              "outcome": {
                "const": "partial"
              }
            },
            "required": [
              "outcome"
            ]
          },
          "then": {
            "required": [
              "diagnostics"
            ]
          }
        }
      ],
      "description": "Observation Activity/PREMIS Event that used a file state and technical environment to generate provenance assertions. It does not generate the file state.",
      "properties": {
        "associations": {
          "contains": {
            "properties": {
              "role": {
                "const": "executing_software"
              }
            },
            "required": [
              "role"
            ],
            "type": "object"
          },
          "items": {
            "$ref": "#/$defs/association"
          },
          "minContains": 1,
          "minItems": 1,
          "type": "array",
          "uniqueItems": true
        },
        "consistency": {
          "enum": [
            "snapshot",
            "verified_unchanged",
            "best_effort",
            "unknown"
          ],
          "type": "string"
        },
        "coverage": {
          "$ref": "#/$defs/coverage"
        },
        "detail": {
          "$ref": "#/$defs/captureDetail"
        },
        "diagnostics": {
          "items": {
            "$ref": "#/$defs/diagnostic"
          },
          "minItems": 1,
          "type": "array"
        },
        "ended_at": {
          "$ref": "#/$defs/utcDateTime"
        },
        "environment_id": {
          "$ref": "#/$defs/urnUuid"
        },
        "id": {
          "$ref": "#/$defs/urnUuid"
        },
        "notes": {
          "items": {
            "$ref": "#/$defs/nonEmptyString"
          },
          "minItems": 1,
          "type": "array",
          "uniqueItems": true
        },
        "operations": {
          "allOf": [
            {
              "contains": {
                "const": "metadata_extraction"
              },
              "minContains": 1
            },
            {
              "contains": {
                "const": "message_digest_calculation"
              },
              "minContains": 1
            }
          ],
          "items": {
            "enum": [
              "metadata_extraction",
              "message_digest_calculation",
              "principal_resolution"
            ],
            "type": "string"
          },
          "minItems": 2,
          "type": "array",
          "uniqueItems": true
        },
        "outcome": {
          "enum": [
            "success",
            "partial"
          ],
          "type": "string"
        },
        "started_at": {
          "$ref": "#/$defs/utcDateTime"
        },
        "state_id": {
          "$ref": "#/$defs/urnUuid"
        },
        "type": {
          "const": "file_state_capture"
        }
      },
      "required": [
        "id",
        "type",
        "state_id",
        "operations",
        "started_at",
        "ended_at",
        "outcome",
        "consistency",
        "environment_id",
        "associations",
        "coverage"
      ],
      "type": "object"
    },
    "capturedValue": {
      "oneOf": [
        {
          "$ref": "#/$defs/bytesValue"
        },
        {
          "$ref": "#/$defs/textValue"
        },
        {
          "$ref": "#/$defs/integerValue"
        },
        {
          "$ref": "#/$defs/decimalValue"
        },
        {
          "$ref": "#/$defs/booleanValue"
        },
        {
          "$ref": "#/$defs/timestampValue"
        },
        {
          "$ref": "#/$defs/uriValue"
        },
        {
          "$ref": "#/$defs/entityReferenceValue"
        },
        {
          "$ref": "#/$defs/jsonValue"
        }
      ]
    },
    "checkpointBody": {
      "additionalProperties": false,
      "allOf": [
        {
          "if": {
            "properties": {
              "checkpoint_kind": {
                "const": "other"
              }
            },
            "required": [
              "checkpoint_kind"
            ]
          },
          "then": {
            "required": [
              "checkpoint_kind_uri"
            ]
          }
        }
      ],
      "description": "A cumulative checkpoint over exact RFC 7464 bytes through the preceding entry. Tail truncation is detectable only when a checkpoint or tail digest is anchored externally.",
      "properties": {
        "checkpoint_kind": {
          "enum": [
            "periodic",
            "transfer",
            "archive_ingest",
            "publication",
            "other"
          ],
          "type": "string"
        },
        "checkpoint_kind_uri": {
          "$ref": "#/$defs/absoluteUri"
        },
        "counts": {
          "$ref": "#/$defs/checkpointCounts"
        },
        "covered_through": {
          "$ref": "#/$defs/entryReference"
        },
        "note": {
          "$ref": "#/$defs/nonEmptyString"
        },
        "stream_prefix_sha256": {
          "$ref": "#/$defs/sha256Hex"
        }
      },
      "required": [
        "checkpoint_kind",
        "covered_through",
        "stream_prefix_sha256",
        "counts"
      ],
      "type": "object"
    },
    "checkpointCounts": {
      "additionalProperties": false,
      "properties": {
        "activities": {
          "maximum": "9223372036854775807",
          "minimum": 0,
          "type": "integer"
        },
        "captures": {
          "maximum": "9223372036854775807",
          "minimum": 0,
          "type": "integer"
        },
        "entries": {
          "maximum": "9223372036854775807",
          "minimum": 1,
          "type": "integer"
        },
        "lineages": {
          "maximum": "9223372036854775807",
          "minimum": 0,
          "type": "integer"
        },
        "relations": {
          "maximum": "9223372036854775807",
          "minimum": 0,
          "type": "integer"
        },
        "states": {
          "maximum": "9223372036854775807",
          "minimum": 0,
          "type": "integer"
        }
      },
      "required": [
        "entries"
      ],
      "type": "object"
    },
    "comparisonDimension": {
      "additionalProperties": false,
      "properties": {
        "basis": {
          "items": {
            "enum": [
              "size",
              "sha-256",
              "exact_json_value",
              "byte_exact",
              "normalized_value",
              "complete_capture",
              "producer_assertion"
            ],
            "type": "string"
          },
          "minItems": 1,
          "type": "array",
          "uniqueItems": true
        },
        "dimension": {
          "enum": [
            "primary_content",
            "locator",
            "timestamps",
            "ownership",
            "permissions",
            "native_identifiers",
            "native_metadata"
          ],
          "type": "string"
        },
        "note": {
          "$ref": "#/$defs/nonEmptyString"
        },
        "result": {
          "enum": [
            "matching_fixity",
            "equal",
            "different",
            "not_observed",
            "not_comparable",
            "indeterminate"
          ],
          "type": "string"
        }
      },
      "required": [
        "dimension",
        "result",
        "basis"
      ],
      "type": "object"
    },
    "comparisonRelation": {
      "additionalProperties": false,
      "properties": {
        "asserted_by_agent_id": {
          "$ref": "#/$defs/urnUuid"
        },
        "compared_at": {
          "$ref": "#/$defs/utcDateTime"
        },
        "confidence": {
          "enum": [
            "high",
            "medium",
            "low",
            "unknown"
          ],
          "type": "string"
        },
        "dimensions": {
          "items": {
            "$ref": "#/$defs/comparisonDimension"
          },
          "minItems": 1,
          "type": "array",
          "uniqueItems": true
        },
        "from_capture_id": {
          "$ref": "#/$defs/urnUuid"
        },
        "from_state": {
          "$ref": "#/$defs/stateReference"
        },
        "id": {
          "$ref": "#/$defs/urnUuid"
        },
        "notes": {
          "items": {
            "$ref": "#/$defs/nonEmptyString"
          },
          "minItems": 1,
          "type": "array",
          "uniqueItems": true
        },
        "to_capture_id": {
          "$ref": "#/$defs/urnUuid"
        },
        "to_state": {
          "$ref": "#/$defs/stateReference"
        },
        "type": {
          "const": "state_comparison"
        }
      },
      "required": [
        "id",
        "type",
        "from_state",
        "to_state",
        "from_capture_id",
        "to_capture_id",
        "compared_at",
        "dimensions",
        "asserted_by_agent_id",
        "confidence"
      ],
      "type": "object"
    },
    "contentDescription": {
      "additionalProperties": false,
      "description": "Opaque primary byte-stream observations only: length and cryptographic fixity. Internal format, codec, dimensions, duration, and other byte semantics are outside the core profile.",
      "properties": {
        "digests": {
          "contains": {
            "properties": {
              "algorithm": {
                "const": "sha-256"
              },
              "purpose": {
                "const": "fixity"
              }
            },
            "required": [
              "algorithm",
              "purpose"
            ],
            "type": "object"
          },
          "items": {
            "$ref": "#/$defs/digest"
          },
          "maxContains": 1,
          "minContains": 1,
          "minItems": 1,
          "type": "array",
          "uniqueItems": true
        },
        "size_bytes": {
          "maximum": "9223372036854775807",
          "minimum": 0,
          "type": "integer"
        }
      },
      "required": [
        "size_bytes",
        "digests"
      ],
      "type": "object"
    },
    "continuityBasis": {
      "additionalProperties": false,
      "allOf": [
        {
          "if": {
            "properties": {
              "type": {
                "const": "other"
              }
            },
            "required": [
              "type"
            ]
          },
          "then": {
            "required": [
              "uri"
            ]
          }
        }
      ],
      "properties": {
        "type": {
          "enum": [
            "native_file_identity",
            "snapshot_identity",
            "repository_tracking",
            "process_record",
            "operator_assertion",
            "matching_fixity_and_locator",
            "other"
          ],
          "type": "string"
        },
        "uri": {
          "$ref": "#/$defs/absoluteUri"
        }
      },
      "required": [
        "type"
      ],
      "type": "object"
    },
    "continuityRelation": {
      "additionalProperties": false,
      "properties": {
        "asserted_by_agent_id": {
          "$ref": "#/$defs/urnUuid"
        },
        "basis": {
          "items": {
            "$ref": "#/$defs/continuityBasis"
          },
          "minItems": 1,
          "type": "array",
          "uniqueItems": true
        },
        "confidence": {
          "enum": [
            "high",
            "medium",
            "low",
            "unknown"
          ],
          "type": "string"
        },
        "continuity_kind": {
          "enum": [
            "reobservation",
            "same_source_instance",
            "declared_successor"
          ],
          "type": "string"
        },
        "from_state": {
          "$ref": "#/$defs/stateReference"
        },
        "id": {
          "$ref": "#/$defs/urnUuid"
        },
        "note": {
          "$ref": "#/$defs/nonEmptyString"
        },
        "to_state": {
          "$ref": "#/$defs/stateReference"
        },
        "type": {
          "const": "state_continuity"
        }
      },
      "required": [
        "id",
        "type",
        "from_state",
        "to_state",
        "continuity_kind",
        "basis",
        "asserted_by_agent_id",
        "confidence"
      ],
      "type": "object"
    },
    "correctionBody": {
      "additionalProperties": false,
      "allOf": [
        {
          "if": {
            "properties": {
              "action": {
                "const": "replace"
              }
            },
            "required": [
              "action"
            ]
          },
          "then": {
            "required": [
              "replacement"
            ]
          }
        },
        {
          "if": {
            "properties": {
              "action": {
                "const": "retract"
              }
            },
            "required": [
              "action"
            ]
          },
          "then": {
            "not": {
              "required": [
                "replacement"
              ]
            }
          }
        }
      ],
      "description": "Monotonic correction of prior assertion entries. Supersession changes the effective provenance description; it never denotes a change to the file itself.",
      "properties": {
        "action": {
          "enum": [
            "replace",
            "retract"
          ],
          "type": "string"
        },
        "reason": {
          "$ref": "#/$defs/nonEmptyString"
        },
        "replacement": {
          "$ref": "#/$defs/graphFragment"
        },
        "supersedes": {
          "items": {
            "$ref": "#/$defs/entryReference"
          },
          "minItems": 1,
          "type": "array",
          "uniqueItems": true
        }
      },
      "required": [
        "action",
        "reason",
        "supersedes"
      ],
      "type": "object"
    },
    "coverage": {
      "additionalProperties": false,
      "description": "Exhaustiveness status for every filesystem capture category. Complete concerns accountable enumeration under the declared plan, not universal retention.",
      "properties": {
        "access_control": {
          "$ref": "#/$defs/coverageStatus"
        },
        "alternate_streams": {
          "$ref": "#/$defs/coverageStatus"
        },
        "basic_filesystem": {
          "const": "complete"
        },
        "content_fixity": {
          "const": "complete"
        },
        "extended_attributes": {
          "$ref": "#/$defs/coverageStatus"
        },
        "file_flags": {
          "$ref": "#/$defs/coverageStatus"
        },
        "locator": {
          "const": "complete"
        },
        "native_identifiers": {
          "$ref": "#/$defs/coverageStatus"
        },
        "native_metadata_other": {
          "$ref": "#/$defs/coverageStatus"
        },
        "ownership": {
          "$ref": "#/$defs/coverageStatus"
        },
        "permissions": {
          "$ref": "#/$defs/coverageStatus"
        },
        "resource_forks": {
          "$ref": "#/$defs/coverageStatus"
        },
        "security_metadata": {
          "$ref": "#/$defs/coverageStatus"
        },
        "special_file_features": {
          "$ref": "#/$defs/coverageStatus"
        },
        "storage_layout": {
          "$ref": "#/$defs/coverageStatus"
        },
        "timestamps": {
          "$ref": "#/$defs/coverageStatus"
        }
      },
      "required": [
        "content_fixity",
        "locator",
        "basic_filesystem",
        "timestamps",
        "ownership",
        "permissions",
        "native_identifiers",
        "extended_attributes",
        "access_control",
        "alternate_streams",
        "resource_forks",
        "file_flags",
        "security_metadata",
        "storage_layout",
        "special_file_features",
        "native_metadata_other"
      ],
      "type": "object"
    },
    "coverageStatus": {
      "enum": [
        "complete",
        "partial",
        "not_supported",
        "not_applicable",
        "not_requested",
        "failed"
      ],
      "type": "string"
    },
    "decimalValue": {
      "additionalProperties": false,
      "properties": {
        "data": {
          "pattern": "^-?(?:0|[1-9][0-9]*)(?:\\.[0-9]+)?$",
          "type": "string"
        },
        "type": {
          "const": "decimal"
        }
      },
      "required": [
        "type",
        "data"
      ],
      "type": "object"
    },
    "derivationRelation": {
      "additionalProperties": false,
      "allOf": [
        {
          "if": {
            "properties": {
              "derivation_kind": {
                "const": "other"
              }
            },
            "required": [
              "derivation_kind"
            ]
          },
          "then": {
            "required": [
              "derivation_kind_uri"
            ]
          }
        }
      ],
      "properties": {
        "activity_id": {
          "$ref": "#/$defs/urnUuid"
        },
        "derivation_kind": {
          "enum": [
            "revision",
            "transformation",
            "copy",
            "metadata_change",
            "relocation",
            "aggregation",
            "extraction",
            "other"
          ],
          "type": "string"
        },
        "derivation_kind_uri": {
          "$ref": "#/$defs/absoluteUri"
        },
        "generated_state": {
          "$ref": "#/$defs/stateReference"
        },
        "id": {
          "$ref": "#/$defs/urnUuid"
        },
        "type": {
          "const": "derivation"
        },
        "used_state": {
          "$ref": "#/$defs/stateReference"
        }
      },
      "required": [
        "id",
        "type",
        "generated_state",
        "used_state",
        "activity_id",
        "derivation_kind"
      ],
      "type": "object"
    },
    "diagnostic": {
      "additionalProperties": false,
      "properties": {
        "category": {
          "enum": [
            "general",
            "content_fixity",
            "locator",
            "basic_filesystem",
            "timestamps",
            "ownership",
            "permissions",
            "native_identifiers",
            "extended_attributes",
            "access_control",
            "alternate_streams",
            "resource_forks",
            "file_flags",
            "security_metadata",
            "storage_layout",
            "special_file_features",
            "native_metadata_other"
          ],
          "type": "string"
        },
        "code": {
          "maxLength": 255,
          "minLength": 1,
          "pattern": "^[^\\u0000\\uD800-\\uDFFF]+$",
          "type": "string"
        },
        "message": {
          "$ref": "#/$defs/nonEmptyString"
        },
        "native_code": {
          "$ref": "#/$defs/nonEmptyString"
        },
        "severity": {
          "enum": [
            "warning",
            "error"
          ],
          "type": "string"
        },
        "source": {
          "$ref": "#/$defs/sourceDescriptor"
        }
      },
      "required": [
        "severity",
        "category",
        "code",
        "message"
      ],
      "type": "object"
    },
    "digest": {
      "additionalProperties": false,
      "allOf": [
        {
          "if": {
            "properties": {
              "algorithm": {
                "const": "sha-256"
              }
            },
            "required": [
              "algorithm"
            ]
          },
          "then": {
            "properties": {
              "value": {
                "pattern": "^[0-9a-f]{64}$",
                "type": "string"
              }
            }
          }
        },
        {
          "if": {
            "properties": {
              "algorithm": {
                "const": "sha-512"
              }
            },
            "required": [
              "algorithm"
            ]
          },
          "then": {
            "properties": {
              "value": {
                "pattern": "^[0-9a-f]{128}$",
                "type": "string"
              }
            }
          }
        },
        {
          "if": {
            "properties": {
              "algorithm": {
                "not": {
                  "enum": [
                    "sha-256",
                    "sha-512"
                  ]
                }
              }
            },
            "required": [
              "algorithm"
            ]
          },
          "then": {
            "required": [
              "algorithm_uri"
            ]
          }
        },
        {
          "if": {
            "properties": {
              "purpose": {
                "const": "other"
              }
            },
            "required": [
              "purpose"
            ]
          },
          "then": {
            "required": [
              "purpose_uri"
            ]
          }
        }
      ],
      "description": "A cryptographic digest assertion. Content digests identify byte equality evidence, not file-entity identity.",
      "properties": {
        "algorithm": {
          "pattern": "^[a-z0-9][a-z0-9.-]{0,63}$",
          "type": "string"
        },
        "algorithm_uri": {
          "$ref": "#/$defs/absoluteUri"
        },
        "encoding": {
          "const": "hex"
        },
        "originator_agent_id": {
          "$ref": "#/$defs/urnUuid"
        },
        "purpose": {
          "enum": [
            "fixity",
            "native_metadata",
            "executable",
            "configuration",
            "evidence",
            "journal_entry",
            "other"
          ],
          "type": "string"
        },
        "purpose_uri": {
          "$ref": "#/$defs/absoluteUri"
        },
        "value": {
          "pattern": "^[0-9a-f]+$",
          "type": "string"
        }
      },
      "required": [
        "algorithm",
        "encoding",
        "value",
        "purpose",
        "originator_agent_id"
      ],
      "type": "object"
    },
    "digestOnlyValue": {
      "additionalProperties": false,
      "properties": {
        "byte_length": {
          "maximum": "9223372036854775807",
          "minimum": 0,
          "type": "integer"
        },
        "digests": {
          "items": {
            "$ref": "#/$defs/digest"
          },
          "minItems": 1,
          "type": "array",
          "uniqueItems": true
        },
        "type": {
          "const": "digest"
        }
      },
      "required": [
        "type",
        "byte_length",
        "digests"
      ],
      "type": "object"
    },
    "entityReferenceValue": {
      "additionalProperties": false,
      "properties": {
        "data": {
          "$ref": "#/$defs/absoluteUri"
        },
        "type": {
          "const": "entity_reference"
        }
      },
      "required": [
        "type",
        "data"
      ],
      "type": "object"
    },
    "entryReference": {
      "additionalProperties": false,
      "description": "Reference to the exact UTF-8 JSON-text octets of an earlier journal entry, excluding RFC 7464 RS/LF framing.",
      "properties": {
        "entry_id": {
          "$ref": "#/$defs/urnUuid"
        },
        "json_sha256": {
          "$ref": "#/$defs/sha256Hex"
        },
        "sequence": {
          "maximum": "9223372036854775807",
          "minimum": 0,
          "type": "integer"
        }
      },
      "required": [
        "entry_id",
        "sequence",
        "json_sha256"
      ],
      "type": "object"
    },
    "environment": {
      "additionalProperties": false,
      "description": "Host/OS/filesystem/runtime state used by an observation or transition; a PROV Entity, not an Agent merely because execution occurred there.",
      "properties": {
        "filesystem": {
          "$ref": "#/$defs/filesystem"
        },
        "host": {
          "$ref": "#/$defs/host"
        },
        "id": {
          "$ref": "#/$defs/urnUuid"
        },
        "operating_system": {
          "$ref": "#/$defs/operatingSystem"
        },
        "runtime": {
          "$ref": "#/$defs/runtime"
        },
        "type": {
          "const": "technical_environment"
        }
      },
      "required": [
        "id",
        "type",
        "host",
        "operating_system",
        "filesystem",
        "runtime"
      ],
      "type": "object"
    },
    "fieldSourceDescriptor": {
      "allOf": [
        {
          "$ref": "#/$defs/sourceDescriptor"
        },
        {
          "required": [
            "field"
          ]
        }
      ]
    },
    "fileLineage": {
      "additionalProperties": false,
      "allOf": [
        {
          "if": {
            "properties": {
              "continuity_basis": {
                "const": "other"
              }
            },
            "required": [
              "continuity_basis"
            ]
          },
          "then": {
            "required": [
              "continuity_basis_uri"
            ]
          }
        }
      ],
      "description": "An abstract continuing file lineage. Each immutable state is a specialization of one lineage; grouping is an attributable continuity assertion, not inferred from path or digest alone.",
      "properties": {
        "asserted_by_agent_id": {
          "$ref": "#/$defs/urnUuid"
        },
        "continuity_basis": {
          "enum": [
            "producer_declared",
            "repository_tracking",
            "source_native_tracking",
            "process_provenance",
            "other"
          ],
          "type": "string"
        },
        "continuity_basis_uri": {
          "$ref": "#/$defs/absoluteUri"
        },
        "id": {
          "$ref": "#/$defs/urnUuid"
        },
        "identifiers": {
          "items": {
            "$ref": "#/$defs/identifier"
          },
          "minItems": 1,
          "type": "array",
          "uniqueItems": true
        },
        "label": {
          "$ref": "#/$defs/nonEmptyString"
        },
        "notes": {
          "items": {
            "$ref": "#/$defs/nonEmptyString"
          },
          "minItems": 1,
          "type": "array",
          "uniqueItems": true
        },
        "type": {
          "const": "file_lineage"
        }
      },
      "required": [
        "id",
        "type",
        "continuity_basis",
        "asserted_by_agent_id"
      ],
      "type": "object"
    },
    "fileState": {
      "additionalProperties": false,
      "description": "One immutable, full observed snapshot of one operating-system regular-file state; a PROV Entity and PREMIS File.",
      "properties": {
        "content": {
          "$ref": "#/$defs/contentDescription"
        },
        "filesystem_metadata": {
          "$ref": "#/$defs/filesystemMetadata"
        },
        "id": {
          "$ref": "#/$defs/urnUuid"
        },
        "lineage_id": {
          "$ref": "#/$defs/urnUuid"
        },
        "locator": {
          "$ref": "#/$defs/locator"
        },
        "notes": {
          "items": {
            "$ref": "#/$defs/nonEmptyString"
          },
          "minItems": 1,
          "type": "array",
          "uniqueItems": true
        },
        "type": {
          "const": "regular_file_state"
        }
      },
      "required": [
        "id",
        "type",
        "lineage_id",
        "locator",
        "content",
        "filesystem_metadata"
      ],
      "type": "object"
    },
    "filesystem": {
      "additionalProperties": false,
      "properties": {
        "case_preserving": {
          "type": "boolean"
        },
        "case_sensitive": {
          "type": "boolean"
        },
        "mount_locator": {
          "$ref": "#/$defs/locator"
        },
        "name_normalization": {
          "enum": [
            "none",
            "nfc",
            "nfd",
            "nfkc",
            "nfkd",
            "implementation_defined",
            "unknown"
          ],
          "type": "string"
        },
        "networked": {
          "type": "boolean"
        },
        "snapshot_identifiers": {
          "items": {
            "$ref": "#/$defs/identifier"
          },
          "minItems": 1,
          "type": "array",
          "uniqueItems": true
        },
        "type": {
          "maxLength": 128,
          "minLength": 1,
          "pattern": "^[^\\u0000\\uD800-\\uDFFF]+$",
          "type": "string"
        },
        "type_uri": {
          "$ref": "#/$defs/absoluteUri"
        },
        "version": {
          "$ref": "#/$defs/nonEmptyString"
        },
        "volume_identifiers": {
          "items": {
            "$ref": "#/$defs/identifier"
          },
          "minItems": 1,
          "type": "array",
          "uniqueItems": true
        }
      },
      "required": [
        "type"
      ],
      "type": "object"
    },
    "filesystemMetadata": {
      "additionalProperties": false,
      "properties": {
        "access": {
          "$ref": "#/$defs/accessMetadata"
        },
        "native_identifiers": {
          "items": {
            "$ref": "#/$defs/observedIdentifier"
          },
          "type": "array",
          "uniqueItems": true
        },
        "native_metadata": {
          "items": {
            "$ref": "#/$defs/nativeMetadata"
          },
          "type": "array",
          "uniqueItems": true
        },
        "timestamps": {
          "items": {
            "$ref": "#/$defs/timestampObservation"
          },
          "type": "array",
          "uniqueItems": true
        }
      },
      "required": [
        "timestamps",
        "native_identifiers",
        "native_metadata"
      ],
      "type": "object"
    },
    "generationRelation": {
      "additionalProperties": false,
      "allOf": [
        {
          "if": {
            "properties": {
              "role": {
                "const": "other"
              }
            },
            "required": [
              "role"
            ]
          },
          "then": {
            "required": [
              "role_uri"
            ]
          }
        }
      ],
      "properties": {
        "activity_id": {
          "$ref": "#/$defs/urnUuid"
        },
        "id": {
          "$ref": "#/$defs/urnUuid"
        },
        "role": {
          "$ref": "#/$defs/relationRole"
        },
        "role_uri": {
          "$ref": "#/$defs/absoluteUri"
        },
        "state": {
          "$ref": "#/$defs/stateReference"
        },
        "type": {
          "const": "generation"
        }
      },
      "required": [
        "id",
        "type",
        "activity_id",
        "state",
        "role"
      ],
      "type": "object"
    },
    "graphFragment": {
      "additionalProperties": false,
      "description": "An immutable atomic bundle of graph assertions. Arrays are semantic sets; array order does not imply chronology or causality.",
      "minProperties": 1,
      "properties": {
        "activities": {
          "items": {
            "$ref": "#/$defs/transitionActivity"
          },
          "minItems": 1,
          "type": "array",
          "uniqueItems": true
        },
        "agents": {
          "items": {
            "$ref": "#/$defs/agent"
          },
          "minItems": 1,
          "type": "array",
          "uniqueItems": true
        },
        "captures": {
          "items": {
            "$ref": "#/$defs/captureEvent"
          },
          "minItems": 1,
          "type": "array",
          "uniqueItems": true
        },
        "environments": {
          "items": {
            "$ref": "#/$defs/environment"
          },
          "minItems": 1,
          "type": "array",
          "uniqueItems": true
        },
        "extensions": {
          "items": {
            "$ref": "#/$defs/semanticAssertion"
          },
          "minItems": 1,
          "type": "array",
          "uniqueItems": true
        },
        "lineages": {
          "items": {
            "$ref": "#/$defs/fileLineage"
          },
          "minItems": 1,
          "type": "array",
          "uniqueItems": true
        },
        "payload_bindings": {
          "items": {
            "$ref": "#/$defs/payloadBinding"
          },
          "minItems": 1,
          "type": "array",
          "uniqueItems": true
        },
        "relations": {
          "items": {
            "$ref": "#/$defs/relation"
          },
          "minItems": 1,
          "type": "array",
          "uniqueItems": true
        },
        "states": {
          "items": {
            "$ref": "#/$defs/fileState"
          },
          "minItems": 1,
          "type": "array",
          "uniqueItems": true
        }
      },
      "type": "object"
    },
    "hardwareAgent": {
      "additionalProperties": false,
      "properties": {
        "id": {
          "$ref": "#/$defs/urnUuid"
        },
        "identifiers": {
          "items": {
            "$ref": "#/$defs/identifier"
          },
          "minItems": 1,
          "type": "array",
          "uniqueItems": true
        },
        "model": {
          "$ref": "#/$defs/nonEmptyString"
        },
        "name": {
          "$ref": "#/$defs/nonEmptyString"
        },
        "type": {
          "const": "hardware"
        },
        "vendor": {
          "$ref": "#/$defs/nonEmptyString"
        },
        "version": {
          "$ref": "#/$defs/nonEmptyString"
        }
      },
      "required": [
        "id",
        "type",
        "name"
      ],
      "type": "object"
    },
    "host": {
      "additionalProperties": false,
      "properties": {
        "hardware_architecture": {
          "$ref": "#/$defs/nonEmptyString"
        },
        "hardware_model": {
          "$ref": "#/$defs/nonEmptyString"
        },
        "id": {
          "$ref": "#/$defs/urnUuid"
        },
        "identifiers": {
          "items": {
            "$ref": "#/$defs/identifier"
          },
          "minItems": 1,
          "type": "array",
          "uniqueItems": true
        },
        "name": {
          "$ref": "#/$defs/nonEmptyString"
        }
      },
      "required": [
        "id"
      ],
      "type": "object"
    },
    "identifier": {
      "additionalProperties": false,
      "allOf": [
        {
          "if": {
            "properties": {
              "representation": {
                "enum": [
                  "sha256",
                  "hmac_sha256"
                ]
              }
            },
            "required": [
              "representation"
            ]
          },
          "then": {
            "properties": {
              "value": {
                "pattern": "^[0-9a-f]{64}$",
                "type": "string"
              }
            }
          }
        },
        {
          "if": {
            "properties": {
              "scope": {
                "not": {
                  "const": "global"
                }
              }
            },
            "required": [
              "scope"
            ]
          },
          "then": {
            "required": [
              "authority_id"
            ]
          }
        },
        {
          "if": {
            "properties": {
              "representation": {
                "const": "hmac_sha256"
              }
            },
            "required": [
              "representation"
            ]
          },
          "then": {
            "required": [
              "authority_id"
            ]
          }
        }
      ],
      "properties": {
        "authority_id": {
          "$ref": "#/$defs/absoluteUri"
        },
        "representation": {
          "enum": [
            "clear",
            "sha256",
            "hmac_sha256"
          ],
          "type": "string"
        },
        "scheme": {
          "maxLength": 255,
          "minLength": 1,
          "pattern": "^[^\\u0000\\uD800-\\uDFFF]+$",
          "type": "string"
        },
        "scope": {
          "enum": [
            "record",
            "repository",
            "host",
            "volume",
            "filesystem",
            "organization",
            "global"
          ],
          "type": "string"
        },
        "value": {
          "minLength": 1,
          "pattern": "^[^\\u0000\\uD800-\\uDFFF]+$",
          "type": "string"
        }
      },
      "required": [
        "scheme",
        "value",
        "scope",
        "representation"
      ],
      "type": "object"
    },
    "integerValue": {
      "additionalProperties": false,
      "properties": {
        "data": {
          "pattern": "^-?(?:0|[1-9][0-9]*)$",
          "type": "string"
        },
        "type": {
          "const": "integer"
        }
      },
      "required": [
        "type",
        "data"
      ],
      "type": "object"
    },
    "interpretation": {
      "additionalProperties": false,
      "allOf": [
        {
          "if": {
            "properties": {
              "kind": {
                "const": "text_decode"
              }
            },
            "required": [
              "kind"
            ]
          },
          "then": {
            "properties": {
              "value": {
                "$ref": "#/$defs/textValue"
              }
            }
          }
        },
        {
          "if": {
            "properties": {
              "kind": {
                "enum": [
                  "structured_parse",
                  "other"
                ]
              }
            },
            "required": [
              "kind"
            ]
          },
          "then": {
            "required": [
              "schema"
            ]
          }
        }
      ],
      "properties": {
        "agent_id": {
          "$ref": "#/$defs/urnUuid"
        },
        "confidence": {
          "enum": [
            "high",
            "medium",
            "low",
            "unknown"
          ],
          "type": "string"
        },
        "kind": {
          "enum": [
            "text_decode",
            "structured_parse",
            "normalized_value",
            "other"
          ],
          "type": "string"
        },
        "note": {
          "$ref": "#/$defs/nonEmptyString"
        },
        "schema": {
          "$ref": "#/$defs/absoluteUri"
        },
        "value": {
          "$ref": "#/$defs/capturedValue"
        }
      },
      "required": [
        "kind",
        "value",
        "agent_id",
        "confidence"
      ],
      "type": "object"
    },
    "invalidationRelation": {
      "additionalProperties": false,
      "properties": {
        "activity_id": {
          "$ref": "#/$defs/urnUuid"
        },
        "id": {
          "$ref": "#/$defs/urnUuid"
        },
        "reason": {
          "$ref": "#/$defs/nonEmptyString"
        },
        "state": {
          "$ref": "#/$defs/stateReference"
        },
        "type": {
          "const": "invalidation"
        }
      },
      "required": [
        "id",
        "type",
        "activity_id",
        "state",
        "reason"
      ],
      "type": "object"
    },
    "journalInitBody": {
      "additionalProperties": false,
      "properties": {
        "assertions": {
          "$ref": "#/$defs/graphFragment"
        },
        "journal": {
          "$ref": "#/$defs/journalPolicy"
        }
      },
      "required": [
        "journal",
        "assertions"
      ],
      "type": "object"
    },
    "journalPolicy": {
      "additionalProperties": false,
      "allOf": [
        {
          "if": {
            "properties": {
              "retention_intent": {
                "const": "other"
              }
            },
            "required": [
              "retention_intent"
            ]
          },
          "then": {
            "required": [
              "retention_intent_uri"
            ]
          }
        }
      ],
      "properties": {
        "correction_model": {
          "const": "monotonic_entry_supersession"
        },
        "entry_digest_algorithm": {
          "const": "sha-256"
        },
        "entry_digest_coverage": {
          "const": "json_text_octets_excluding_framing"
        },
        "label": {
          "$ref": "#/$defs/nonEmptyString"
        },
        "payload_semantics": {
          "const": "opaque_bytes"
        },
        "primary_lineage_id": {
          "$ref": "#/$defs/urnUuid"
        },
        "retention_intent": {
          "enum": [
            "permanent_archival",
            "long_term_preservation",
            "operational_provenance",
            "other"
          ],
          "type": "string"
        },
        "retention_intent_uri": {
          "$ref": "#/$defs/absoluteUri"
        },
        "scope": {
          "const": "primary_lineage_with_related_provenance"
        },
        "serialization": {
          "const": "rfc7464_json_text_sequence"
        },
        "state_representation": {
          "const": "full_snapshot"
        }
      },
      "required": [
        "primary_lineage_id",
        "scope",
        "serialization",
        "entry_digest_algorithm",
        "entry_digest_coverage",
        "state_representation",
        "payload_semantics",
        "correction_model",
        "retention_intent"
      ],
      "type": "object"
    },
    "jsonValue": {
      "additionalProperties": false,
      "properties": {
        "data": {
          "$ref": "#/$defs/nonNullJson"
        },
        "schema": {
          "$ref": "#/$defs/absoluteUri"
        },
        "type": {
          "const": "json"
        }
      },
      "required": [
        "type",
        "data",
        "schema"
      ],
      "type": "object"
    },
    "kernel": {
      "additionalProperties": false,
      "minProperties": 1,
      "properties": {
        "name": {
          "$ref": "#/$defs/nonEmptyString"
        },
        "release": {
          "$ref": "#/$defs/nonEmptyString"
        },
        "version": {
          "$ref": "#/$defs/nonEmptyString"
        }
      },
      "type": "object"
    },
    "locator": {
      "additionalProperties": false,
      "allOf": [
        {
          "if": {
            "properties": {
              "syntax": {
                "const": "opaque"
              }
            },
            "required": [
              "syntax"
            ]
          },
          "then": {
            "properties": {
              "kind": {
                "const": "opaque"
              }
            }
          }
        },
        {
          "if": {
            "properties": {
              "kind": {
                "const": "opaque"
              }
            },
            "required": [
              "kind"
            ]
          },
          "then": {
            "properties": {
              "syntax": {
                "const": "opaque"
              }
            }
          }
        },
        {
          "if": {
            "required": [
              "text_role"
            ]
          },
          "then": {
            "required": [
              "text"
            ]
          }
        },
        {
          "if": {
            "properties": {
              "text_role": {
                "const": "display"
              }
            },
            "required": [
              "text_role"
            ]
          },
          "then": {
            "required": [
              "bytes"
            ]
          }
        },
        {
          "if": {
            "required": [
              "text",
              "bytes"
            ]
          },
          "then": {
            "required": [
              "text_role"
            ]
          }
        },
        {
          "if": {
            "properties": {
              "text_role": {
                "const": "exact"
              }
            },
            "required": [
              "text_role",
              "bytes"
            ]
          },
          "then": {
            "required": [
              "source_encoding"
            ]
          }
        }
      ],
      "anyOf": [
        {
          "required": [
            "text"
          ]
        },
        {
          "required": [
            "bytes"
          ]
        }
      ],
      "properties": {
        "authority_id": {
          "$ref": "#/$defs/absoluteUri"
        },
        "bytes": {
          "$ref": "#/$defs/byteString"
        },
        "kind": {
          "enum": [
            "absolute",
            "relative",
            "opaque"
          ],
          "type": "string"
        },
        "source_encoding": {
          "$ref": "#/$defs/nonEmptyString"
        },
        "syntax": {
          "enum": [
            "posix",
            "windows",
            "uri",
            "opaque"
          ],
          "type": "string"
        },
        "text": {
          "$ref": "#/$defs/portableString"
        },
        "text_role": {
          "description": "Required when text and bytes coexist.",
          "enum": [
            "exact",
            "display"
          ],
          "type": "string"
        }
      },
      "required": [
        "syntax",
        "kind"
      ],
      "type": "object"
    },
    "nativeCoverageCategory": {
      "enum": [
        "extended_attributes",
        "access_control",
        "alternate_streams",
        "resource_forks",
        "file_flags",
        "security_metadata",
        "storage_layout",
        "special_file_features",
        "native_metadata_other"
      ],
      "type": "string"
    },
    "nativeMetadata": {
      "additionalProperties": false,
      "allOf": [
        {
          "if": {
            "properties": {
              "kind": {
                "const": "other"
              }
            },
            "required": [
              "kind"
            ]
          },
          "then": {
            "required": [
              "kind_uri"
            ]
          }
        },
        {
          "if": {
            "properties": {
              "capture_status": {
                "const": "captured"
              }
            },
            "required": [
              "capture_status"
            ]
          },
          "then": {
            "properties": {
              "value": {
                "$ref": "#/$defs/capturedValue"
              }
            },
            "required": [
              "value"
            ]
          }
        },
        {
          "if": {
            "properties": {
              "capture_status": {
                "const": "digest_only"
              }
            },
            "required": [
              "capture_status"
            ]
          },
          "then": {
            "properties": {
              "value": {
                "$ref": "#/$defs/digestOnlyValue"
              }
            },
            "required": [
              "value"
            ]
          }
        },
        {
          "if": {
            "properties": {
              "capture_status": {
                "enum": [
                  "redacted",
                  "unreadable",
                  "not_retained"
                ]
              }
            },
            "required": [
              "capture_status"
            ]
          },
          "then": {
            "not": {
              "required": [
                "value"
              ]
            }
          }
        },
        {
          "if": {
            "required": [
              "name_bytes"
            ]
          },
          "then": {
            "required": [
              "name_role"
            ]
          }
        },
        {
          "if": {
            "properties": {
              "name_role": {
                "const": "display"
              }
            },
            "required": [
              "name_role"
            ]
          },
          "then": {
            "required": [
              "name_bytes"
            ]
          }
        },
        {
          "if": {
            "properties": {
              "name_role": {
                "const": "exact"
              }
            },
            "required": [
              "name_role",
              "name_bytes"
            ]
          },
          "then": {
            "required": [
              "name_source_encoding"
            ]
          }
        },
        {
          "if": {
            "properties": {
              "capture_status": {
                "enum": [
                  "redacted",
                  "unreadable",
                  "not_retained"
                ]
              }
            },
            "required": [
              "capture_status"
            ]
          },
          "then": {
            "required": [
              "note"
            ]
          }
        }
      ],
      "description": "Source-native metadata evidence separated from any interpretation and from restoration semantics.",
      "properties": {
        "capture_status": {
          "enum": [
            "captured",
            "digest_only",
            "redacted",
            "unreadable",
            "not_retained"
          ],
          "type": "string"
        },
        "coverage_category": {
          "$ref": "#/$defs/nativeCoverageCategory"
        },
        "interpretations": {
          "items": {
            "$ref": "#/$defs/interpretation"
          },
          "minItems": 1,
          "type": "array",
          "uniqueItems": true
        },
        "kind": {
          "enum": [
            "extended_attribute",
            "windows_extended_attribute",
            "alternate_data_stream",
            "resource_fork",
            "finder_info",
            "acl",
            "security_descriptor",
            "file_flag",
            "capability",
            "security_label",
            "reparse_point",
            "sparse_map",
            "compression_state",
            "encryption_state",
            "native_stat_field",
            "other"
          ],
          "type": "string"
        },
        "kind_uri": {
          "$ref": "#/$defs/absoluteUri"
        },
        "name": {
          "$ref": "#/$defs/nonEmptyString"
        },
        "name_bytes": {
          "$ref": "#/$defs/byteString"
        },
        "name_role": {
          "enum": [
            "exact",
            "display"
          ],
          "type": "string"
        },
        "name_source_encoding": {
          "$ref": "#/$defs/nonEmptyString"
        },
        "namespace": {
          "$ref": "#/$defs/nonEmptyString"
        },
        "note": {
          "$ref": "#/$defs/nonEmptyString"
        },
        "observed_byte_length": {
          "maximum": "9223372036854775807",
          "minimum": 0,
          "type": "integer"
        },
        "sensitivity": {
          "enum": [
            "public",
            "personal",
            "security_sensitive",
            "secret",
            "unknown"
          ],
          "type": "string"
        },
        "source": {
          "$ref": "#/$defs/sourceDescriptor"
        },
        "value": {
          "$ref": "#/$defs/typedValue"
        }
      },
      "required": [
        "kind",
        "coverage_category",
        "name",
        "capture_status",
        "source",
        "sensitivity"
      ],
      "type": "object"
    },
    "nonEmptyString": {
      "minLength": 1,
      "pattern": "^[^\\u0000\\uD800-\\uDFFF]+$",
      "type": "string"
    },
    "nonNullJson": {
      "anyOf": [
        {
          "$ref": "#/$defs/portableString"
        },
        {
          "type": "number"
        },
        {
          "type": "boolean"
        },
        {
          "items": {
            "$ref": "#/$defs/nonNullJson"
          },
          "type": "array"
        },
        {
          "additionalProperties": {
            "$ref": "#/$defs/nonNullJson"
          },
          "propertyNames": {
            "$ref": "#/$defs/portableString"
          },
          "type": "object"
        }
      ]
    },
    "observedIdentifier": {
      "additionalProperties": false,
      "allOf": [
        {
          "if": {
            "properties": {
              "representation": {
                "enum": [
                  "sha256",
                  "hmac_sha256"
                ]
              }
            },
            "required": [
              "representation"
            ]
          },
          "then": {
            "properties": {
              "value": {
                "pattern": "^[0-9a-f]{64}$",
                "type": "string"
              }
            }
          }
        },
        {
          "if": {
            "properties": {
              "scope": {
                "not": {
                  "const": "global"
                }
              }
            },
            "required": [
              "scope"
            ]
          },
          "then": {
            "required": [
              "authority_id"
            ]
          }
        },
        {
          "if": {
            "properties": {
              "representation": {
                "const": "hmac_sha256"
              }
            },
            "required": [
              "representation"
            ]
          },
          "then": {
            "required": [
              "authority_id"
            ]
          }
        }
      ],
      "properties": {
        "authority_id": {
          "$ref": "#/$defs/absoluteUri"
        },
        "representation": {
          "enum": [
            "clear",
            "sha256",
            "hmac_sha256"
          ],
          "type": "string"
        },
        "scheme": {
          "maxLength": 255,
          "minLength": 1,
          "pattern": "^[^\\u0000\\uD800-\\uDFFF]+$",
          "type": "string"
        },
        "scope": {
          "enum": [
            "record",
            "repository",
            "host",
            "volume",
            "filesystem",
            "organization",
            "global"
          ],
          "type": "string"
        },
        "source": {
          "$ref": "#/$defs/sourceDescriptor"
        },
        "value": {
          "minLength": 1,
          "pattern": "^[^\\u0000\\uD800-\\uDFFF]+$",
          "type": "string"
        }
      },
      "required": [
        "scheme",
        "value",
        "scope",
        "representation",
        "source"
      ],
      "type": "object"
    },
    "operatingSystem": {
      "additionalProperties": false,
      "allOf": [
        {
          "if": {
            "properties": {
              "family": {
                "const": "other"
              }
            },
            "required": [
              "family"
            ]
          },
          "then": {
            "required": [
              "family_name"
            ]
          }
        }
      ],
      "properties": {
        "build": {
          "$ref": "#/$defs/nonEmptyString"
        },
        "family": {
          "enum": [
            "linux",
            "windows",
            "macos",
            "freebsd",
            "openbsd",
            "netbsd",
            "illumos",
            "aix",
            "android",
            "ios",
            "solaris",
            "other"
          ],
          "type": "string"
        },
        "family_name": {
          "$ref": "#/$defs/nonEmptyString"
        },
        "identifiers": {
          "items": {
            "$ref": "#/$defs/identifier"
          },
          "minItems": 1,
          "type": "array",
          "uniqueItems": true
        },
        "kernel": {
          "$ref": "#/$defs/kernel"
        },
        "name": {
          "$ref": "#/$defs/nonEmptyString"
        },
        "version": {
          "$ref": "#/$defs/nonEmptyString"
        }
      },
      "required": [
        "family",
        "name",
        "version"
      ],
      "type": "object"
    },
    "organizationAgent": {
      "additionalProperties": false,
      "properties": {
        "id": {
          "$ref": "#/$defs/urnUuid"
        },
        "identifiers": {
          "items": {
            "$ref": "#/$defs/identifier"
          },
          "minItems": 1,
          "type": "array",
          "uniqueItems": true
        },
        "name": {
          "$ref": "#/$defs/nonEmptyString"
        },
        "type": {
          "const": "organization"
        }
      },
      "required": [
        "id",
        "type"
      ],
      "type": "object"
    },
    "payloadBinding": {
      "additionalProperties": false,
      "allOf": [
        {
          "if": {
            "properties": {
              "role": {
                "const": "other"
              }
            },
            "required": [
              "role"
            ]
          },
          "then": {
            "required": [
              "role_uri"
            ]
          }
        },
        {
          "if": {
            "properties": {
              "operation": {
                "const": "bind"
              }
            },
            "required": [
              "operation"
            ]
          },
          "then": {
            "oneOf": [
              {
                "required": [
                  "established_by_capture_id"
                ]
              },
              {
                "required": [
                  "established_by_activity_id"
                ]
              }
            ],
            "required": [
              "state",
              "relative_payload_locator",
              "basis"
            ]
          }
        },
        {
          "if": {
            "properties": {
              "operation": {
                "const": "unbind"
              }
            },
            "required": [
              "operation"
            ]
          },
          "then": {
            "not": {
              "anyOf": [
                {
                  "required": [
                    "state"
                  ]
                },
                {
                  "required": [
                    "relative_payload_locator"
                  ]
                },
                {
                  "required": [
                    "established_by_capture_id"
                  ]
                },
                {
                  "required": [
                    "established_by_activity_id"
                  ]
                },
                {
                  "required": [
                    "basis"
                  ]
                },
                {
                  "required": [
                    "basis_uri"
                  ]
                }
              ]
            },
            "required": [
              "replaces_binding_id"
            ]
          }
        },
        {
          "if": {
            "properties": {
              "basis": {
                "const": "other"
              }
            },
            "required": [
              "basis"
            ]
          },
          "then": {
            "required": [
              "basis_uri"
            ]
          }
        }
      ],
      "description": "Append-only assertion binding a journal state to a payload locator relative to the sidecar. The latest effective event per role determines the current binding.",
      "properties": {
        "asserted_by_agent_id": {
          "$ref": "#/$defs/urnUuid"
        },
        "basis": {
          "enum": [
            "size_and_sha256",
            "repository_assertion",
            "other"
          ],
          "type": "string"
        },
        "basis_uri": {
          "$ref": "#/$defs/absoluteUri"
        },
        "established_by_activity_id": {
          "$ref": "#/$defs/urnUuid"
        },
        "established_by_capture_id": {
          "$ref": "#/$defs/urnUuid"
        },
        "id": {
          "$ref": "#/$defs/urnUuid"
        },
        "note": {
          "$ref": "#/$defs/nonEmptyString"
        },
        "operation": {
          "enum": [
            "bind",
            "unbind"
          ],
          "type": "string"
        },
        "relative_payload_locator": {
          "$ref": "#/$defs/locator"
        },
        "replaces_binding_id": {
          "$ref": "#/$defs/urnUuid"
        },
        "role": {
          "enum": [
            "co_resident_primary_payload",
            "related_payload",
            "other"
          ],
          "type": "string"
        },
        "role_uri": {
          "$ref": "#/$defs/absoluteUri"
        },
        "state": {
          "$ref": "#/$defs/stateReference"
        },
        "type": {
          "const": "payload_binding"
        }
      },
      "required": [
        "id",
        "type",
        "operation",
        "role",
        "asserted_by_agent_id"
      ],
      "type": "object"
    },
    "personAgent": {
      "additionalProperties": false,
      "properties": {
        "id": {
          "$ref": "#/$defs/urnUuid"
        },
        "identifiers": {
          "items": {
            "$ref": "#/$defs/identifier"
          },
          "minItems": 1,
          "type": "array",
          "uniqueItems": true
        },
        "name": {
          "$ref": "#/$defs/nonEmptyString"
        },
        "type": {
          "const": "person"
        }
      },
      "required": [
        "id",
        "type"
      ],
      "type": "object"
    },
    "portableString": {
      "pattern": "^[^\\u0000\\uD800-\\uDFFF]*$",
      "type": "string"
    },
    "principal": {
      "additionalProperties": false,
      "anyOf": [
        {
          "required": [
            "name"
          ]
        },
        {
          "required": [
            "identifiers"
          ]
        }
      ],
      "properties": {
        "identifiers": {
          "items": {
            "$ref": "#/$defs/identifier"
          },
          "minItems": 1,
          "type": "array",
          "uniqueItems": true
        },
        "kind": {
          "enum": [
            "user",
            "group",
            "service",
            "device",
            "unknown"
          ],
          "type": "string"
        },
        "name": {
          "$ref": "#/$defs/nonEmptyString"
        },
        "resolution": {
          "enum": [
            "resolved",
            "unresolved",
            "not_attempted"
          ],
          "type": "string"
        }
      },
      "required": [
        "kind",
        "resolution"
      ],
      "type": "object"
    },
    "processDetail": {
      "additionalProperties": false,
      "minProperties": 1,
      "properties": {
        "command_line": {
          "items": {
            "$ref": "#/$defs/portableString"
          },
          "minItems": 1,
          "type": "array"
        },
        "configuration_digest": {
          "$ref": "#/$defs/digest"
        },
        "external_event_identifier": {
          "$ref": "#/$defs/identifier"
        },
        "plan_id": {
          "$ref": "#/$defs/absoluteUri"
        },
        "working_directory": {
          "$ref": "#/$defs/locator"
        }
      },
      "type": "object"
    },
    "processEvidence": {
      "additionalProperties": false,
      "allOf": [
        {
          "if": {
            "properties": {
              "basis": {
                "const": "other"
              }
            },
            "required": [
              "basis"
            ]
          },
          "then": {
            "required": [
              "basis_uri"
            ]
          }
        },
        {
          "if": {
            "properties": {
              "basis": {
                "const": "inferred_from_state_comparison"
              }
            },
            "required": [
              "basis"
            ]
          },
          "then": {
            "required": [
              "comparison_relation_id"
            ]
          }
        }
      ],
      "description": "Evidence supporting the asserted process type. Activity labels such as transcode are process provenance, never inferred from payload bytes unless explicitly marked as a comparison-based inference.",
      "properties": {
        "asserted_by_agent_id": {
          "$ref": "#/$defs/urnUuid"
        },
        "basis": {
          "enum": [
            "direct_process_record",
            "application_log",
            "operating_system_audit",
            "repository_workflow",
            "user_attestation",
            "imported_provenance",
            "inferred_from_state_comparison",
            "unknown",
            "other"
          ],
          "type": "string"
        },
        "basis_uri": {
          "$ref": "#/$defs/absoluteUri"
        },
        "comparison_relation_id": {
          "$ref": "#/$defs/urnUuid"
        },
        "confidence": {
          "enum": [
            "high",
            "medium",
            "low",
            "unknown"
          ],
          "type": "string"
        },
        "description": {
          "$ref": "#/$defs/nonEmptyString"
        },
        "id": {
          "$ref": "#/$defs/urnUuid"
        },
        "reference": {
          "$ref": "#/$defs/typedValue"
        }
      },
      "required": [
        "id",
        "basis",
        "asserted_by_agent_id",
        "confidence"
      ],
      "type": "object"
    },
    "provenanceContractReference": {
      "additionalProperties": false,
      "properties": {
        "contract_id": {
          "$ref": "#/$defs/nonEmptyString"
        },
        "contract_sha256": {
          "$ref": "#/$defs/sha256Hex"
        },
        "format": {
          "const": "riverhog-provenance-contract-reference/v1"
        },
        "provider": {
          "$ref": "#/$defs/nonEmptyString"
        }
      },
      "required": [
        "format",
        "provider",
        "contract_id",
        "contract_sha256"
      ],
      "type": "object"
    },
    "provenanceObserverReference": {
      "additionalProperties": false,
      "dependentRequired": {
        "distribution": [
          "version"
        ],
        "version": [
          "distribution"
        ]
      },
      "properties": {
        "contract": {
          "$ref": "#/$defs/provenanceContractReference"
        },
        "distribution": {
          "$ref": "#/$defs/nonEmptyString"
        },
        "format": {
          "const": "riverhog-provenance-observer-reference/v1"
        },
        "observer_id": {
          "$ref": "#/$defs/nonEmptyString"
        },
        "provider": {
          "$ref": "#/$defs/nonEmptyString"
        },
        "version": {
          "$ref": "#/$defs/nonEmptyString"
        }
      },
      "required": [
        "format",
        "provider",
        "observer_id",
        "contract"
      ],
      "type": "object"
    },
    "relation": {
      "oneOf": [
        {
          "$ref": "#/$defs/usageRelation"
        },
        {
          "$ref": "#/$defs/generationRelation"
        },
        {
          "$ref": "#/$defs/derivationRelation"
        },
        {
          "$ref": "#/$defs/invalidationRelation"
        },
        {
          "$ref": "#/$defs/continuityRelation"
        },
        {
          "$ref": "#/$defs/comparisonRelation"
        }
      ]
    },
    "relationRole": {
      "enum": [
        "source",
        "input",
        "reference",
        "component",
        "metadata_source",
        "result",
        "output",
        "derivative",
        "replacement",
        "copy",
        "other"
      ],
      "type": "string"
    },
    "runtime": {
      "additionalProperties": false,
      "properties": {
        "character_encoding": {
          "$ref": "#/$defs/nonEmptyString"
        },
        "container": {
          "$ref": "#/$defs/nonEmptyString"
        },
        "effective_principal": {
          "$ref": "#/$defs/principal"
        },
        "identifiers": {
          "items": {
            "$ref": "#/$defs/identifier"
          },
          "minItems": 1,
          "type": "array",
          "uniqueItems": true
        },
        "locale": {
          "$ref": "#/$defs/nonEmptyString"
        },
        "privilege": {
          "enum": [
            "unprivileged",
            "elevated",
            "root",
            "system",
            "unknown"
          ],
          "type": "string"
        },
        "process_architecture": {
          "$ref": "#/$defs/nonEmptyString"
        },
        "time_zone": {
          "$ref": "#/$defs/nonEmptyString"
        },
        "utc_offset": {
          "pattern": "^[+-]\u0028?:[01][0-9]|2[0-3]):[0-5][0-9]$",
          "type": "string"
        }
      },
      "required": [
        "process_architecture",
        "privilege"
      ],
      "type": "object"
    },
    "semanticAssertion": {
      "additionalProperties": false,
      "description": "URI-named semantic assertion over the closed typed-value union. It cannot redefine core Riverhog provenance semantics.",
      "properties": {
        "asserted_by_agent_id": {
          "$ref": "#/$defs/urnUuid"
        },
        "confidence": {
          "enum": [
            "high",
            "medium",
            "low",
            "unknown"
          ],
          "type": "string"
        },
        "id": {
          "$ref": "#/$defs/urnUuid"
        },
        "note": {
          "$ref": "#/$defs/nonEmptyString"
        },
        "property": {
          "$ref": "#/$defs/absoluteUri"
        },
        "subject_id": {
          "$ref": "#/$defs/absoluteUri"
        },
        "type": {
          "const": "semantic_assertion"
        },
        "value": {
          "$ref": "#/$defs/typedValue"
        }
      },
      "required": [
        "id",
        "type",
        "subject_id",
        "property",
        "value",
        "asserted_by_agent_id",
        "confidence"
      ],
      "type": "object"
    },
    "sha256Hex": {
      "pattern": "^[0-9a-f]{64}$",
      "type": "string"
    },
    "softwareAgent": {
      "additionalProperties": false,
      "anyOf": [
        {
          "required": [
            "version"
          ]
        },
        {
          "required": [
            "build"
          ]
        },
        {
          "required": [
            "executable_digests"
          ]
        }
      ],
      "properties": {
        "build": {
          "$ref": "#/$defs/nonEmptyString"
        },
        "executable_digests": {
          "items": {
            "$ref": "#/$defs/digest"
          },
          "minItems": 1,
          "type": "array",
          "uniqueItems": true
        },
        "id": {
          "$ref": "#/$defs/urnUuid"
        },
        "identifiers": {
          "items": {
            "$ref": "#/$defs/identifier"
          },
          "minItems": 1,
          "type": "array",
          "uniqueItems": true
        },
        "name": {
          "$ref": "#/$defs/nonEmptyString"
        },
        "type": {
          "const": "software"
        },
        "vendor": {
          "$ref": "#/$defs/nonEmptyString"
        },
        "version": {
          "$ref": "#/$defs/nonEmptyString"
        }
      },
      "required": [
        "id",
        "type",
        "name"
      ],
      "type": "object"
    },
    "sourceDescriptor": {
      "additionalProperties": false,
      "allOf": [
        {
          "if": {
            "properties": {
              "platform": {
                "const": "other"
              }
            },
            "required": [
              "platform"
            ]
          },
          "then": {
            "required": [
              "platform_name"
            ]
          }
        }
      ],
      "properties": {
        "api": {
          "$ref": "#/$defs/nonEmptyString"
        },
        "api_uri": {
          "$ref": "#/$defs/absoluteUri"
        },
        "field": {
          "$ref": "#/$defs/nonEmptyString"
        },
        "field_uri": {
          "$ref": "#/$defs/absoluteUri"
        },
        "platform": {
          "$ref": "#/$defs/sourcePlatform"
        },
        "platform_name": {
          "$ref": "#/$defs/nonEmptyString"
        },
        "version": {
          "$ref": "#/$defs/nonEmptyString"
        }
      },
      "required": [
        "platform",
        "api"
      ],
      "type": "object"
    },
    "sourcePlatform": {
      "enum": [
        "linux",
        "windows",
        "macos",
        "freebsd",
        "openbsd",
        "netbsd",
        "illumos",
        "aix",
        "posix",
        "android",
        "ios",
        "solaris",
        "other"
      ],
      "type": "string"
    },
    "stateReference": {
      "additionalProperties": false,
      "allOf": [
        {
          "if": {
            "properties": {
              "scope": {
                "const": "local"
              }
            },
            "required": [
              "scope"
            ]
          },
          "then": {
            "not": {
              "anyOf": [
                {
                  "required": [
                    "journal_id"
                  ]
                },
                {
                  "required": [
                    "entry_id"
                  ]
                },
                {
                  "required": [
                    "entry_json_sha256"
                  ]
                },
                {
                  "required": [
                    "sidecar_uri"
                  ]
                }
              ]
            }
          }
        },
        {
          "if": {
            "properties": {
              "scope": {
                "const": "external"
              }
            },
            "required": [
              "scope"
            ]
          },
          "then": {
            "required": [
              "journal_id"
            ]
          }
        }
      ],
      "dependentRequired": {
        "entry_id": [
          "entry_json_sha256"
        ],
        "entry_json_sha256": [
          "entry_id"
        ]
      },
      "properties": {
        "entry_id": {
          "$ref": "#/$defs/urnUuid"
        },
        "entry_json_sha256": {
          "$ref": "#/$defs/sha256Hex"
        },
        "id": {
          "$ref": "#/$defs/urnUuid"
        },
        "journal_id": {
          "$ref": "#/$defs/urnUuid"
        },
        "scope": {
          "enum": [
            "local",
            "external"
          ],
          "type": "string"
        },
        "sidecar_uri": {
          "$ref": "#/$defs/absoluteUri"
        }
      },
      "required": [
        "id",
        "scope"
      ],
      "type": "object"
    },
    "textValue": {
      "additionalProperties": false,
      "dependentRequired": {
        "byte_length": [
          "source_encoding"
        ]
      },
      "properties": {
        "byte_length": {
          "maximum": "9223372036854775807",
          "minimum": 0,
          "type": "integer"
        },
        "data": {
          "$ref": "#/$defs/portableString"
        },
        "language": {
          "pattern": "^[A-Za-z]{2,8}(?:-[A-Za-z0-9]{1,8})*$",
          "type": "string"
        },
        "media_type": {
          "$ref": "#/$defs/nonEmptyString"
        },
        "source_encoding": {
          "$ref": "#/$defs/nonEmptyString"
        },
        "type": {
          "const": "text"
        }
      },
      "required": [
        "type",
        "data"
      ],
      "type": "object"
    },
    "timestampObservation": {
      "additionalProperties": false,
      "allOf": [
        {
          "if": {
            "properties": {
              "kind": {
                "const": "other"
              }
            },
            "required": [
              "kind"
            ]
          },
          "then": {
            "required": [
              "kind_uri"
            ]
          }
        },
        {
          "if": {
            "properties": {
              "value_status": {
                "enum": [
                  "exact",
                  "assumed"
                ]
              }
            },
            "required": [
              "value_status"
            ]
          },
          "then": {
            "required": [
              "value",
              "resolution_ns"
            ]
          }
        },
        {
          "if": {
            "properties": {
              "value_status": {
                "const": "assumed"
              }
            },
            "required": [
              "value_status"
            ]
          },
          "then": {
            "required": [
              "assumption"
            ]
          }
        },
        {
          "if": {
            "properties": {
              "value_status": {
                "const": "unresolved"
              }
            },
            "required": [
              "value_status"
            ]
          },
          "then": {
            "not": {
              "required": [
                "value"
              ]
            },
            "required": [
              "raw_value"
            ]
          }
        },
        {
          "if": {
            "properties": {
              "raw_unit": {
                "const": "other"
              }
            },
            "required": [
              "raw_unit"
            ]
          },
          "then": {
            "required": [
              "raw_unit_name"
            ]
          }
        }
      ],
      "dependentRequired": {
        "raw_epoch": [
          "raw_value"
        ],
        "raw_unit": [
          "raw_value"
        ]
      },
      "properties": {
        "assumption": {
          "$ref": "#/$defs/nonEmptyString"
        },
        "kind": {
          "enum": [
            "created",
            "content_modified",
            "metadata_changed",
            "accessed",
            "backup",
            "archived",
            "other"
          ],
          "type": "string"
        },
        "kind_uri": {
          "$ref": "#/$defs/absoluteUri"
        },
        "raw_epoch": {
          "$ref": "#/$defs/utcDateTime"
        },
        "raw_unit": {
          "enum": [
            "seconds",
            "milliseconds",
            "microseconds",
            "nanoseconds",
            "ticks_100ns",
            "days",
            "other"
          ],
          "type": "string"
        },
        "raw_unit_name": {
          "$ref": "#/$defs/nonEmptyString"
        },
        "raw_value": {
          "$ref": "#/$defs/nonEmptyString"
        },
        "resolution_ns": {
          "maximum": "9223372036854775807",
          "minimum": 1,
          "type": "integer"
        },
        "source": {
          "$ref": "#/$defs/fieldSourceDescriptor"
        },
        "value": {
          "$ref": "#/$defs/utcDateTime"
        },
        "value_status": {
          "enum": [
            "exact",
            "assumed",
            "unresolved"
          ],
          "type": "string"
        }
      },
      "required": [
        "kind",
        "value_status",
        "source"
      ],
      "type": "object"
    },
    "timestampValue": {
      "additionalProperties": false,
      "properties": {
        "data": {
          "$ref": "#/$defs/utcDateTime"
        },
        "resolution_ns": {
          "maximum": "9223372036854775807",
          "minimum": 1,
          "type": "integer"
        },
        "type": {
          "const": "timestamp"
        }
      },
      "required": [
        "type",
        "data"
      ],
      "type": "object"
    },
    "transitionActivity": {
      "additionalProperties": false,
      "allOf": [
        {
          "if": {
            "properties": {
              "event_type": {
                "const": "other"
              }
            },
            "required": [
              "event_type"
            ]
          },
          "then": {
            "required": [
              "event_type_uri"
            ]
          }
        }
      ],
      "description": "A real-world Activity/PREMIS Event that used, generated, or invalidated file states. It is distinct from observation capture.",
      "properties": {
        "associations": {
          "items": {
            "$ref": "#/$defs/association"
          },
          "minItems": 1,
          "type": "array",
          "uniqueItems": true
        },
        "detail": {
          "$ref": "#/$defs/processDetail"
        },
        "environment_id": {
          "$ref": "#/$defs/urnUuid"
        },
        "event_label": {
          "$ref": "#/$defs/nonEmptyString"
        },
        "event_type": {
          "enum": [
            "creation",
            "content_modification",
            "metadata_update",
            "transformation",
            "copy",
            "relocation",
            "deletion",
            "composite",
            "unknown_change",
            "other"
          ],
          "type": "string"
        },
        "event_type_uri": {
          "$ref": "#/$defs/absoluteUri"
        },
        "evidence": {
          "items": {
            "$ref": "#/$defs/processEvidence"
          },
          "minItems": 1,
          "type": "array",
          "uniqueItems": true
        },
        "id": {
          "$ref": "#/$defs/urnUuid"
        },
        "notes": {
          "items": {
            "$ref": "#/$defs/nonEmptyString"
          },
          "minItems": 1,
          "type": "array",
          "uniqueItems": true
        },
        "outcome": {
          "enum": [
            "success",
            "partial",
            "failure",
            "unknown"
          ],
          "type": "string"
        },
        "time": {
          "$ref": "#/$defs/activityTime"
        },
        "type": {
          "const": "file_state_transition"
        }
      },
      "required": [
        "id",
        "type",
        "event_type",
        "time",
        "outcome",
        "evidence"
      ],
      "type": "object"
    },
    "typedValue": {
      "description": "Closed tagged value union.",
      "oneOf": [
        {
          "$ref": "#/$defs/capturedValue"
        },
        {
          "$ref": "#/$defs/digestOnlyValue"
        }
      ]
    },
    "uriValue": {
      "additionalProperties": false,
      "properties": {
        "data": {
          "$ref": "#/$defs/absoluteUri"
        },
        "type": {
          "const": "uri"
        }
      },
      "required": [
        "type",
        "data"
      ],
      "type": "object"
    },
    "urnUuid": {
      "pattern": "^urn:uuid:[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$",
      "type": "string"
    },
    "usageRelation": {
      "additionalProperties": false,
      "allOf": [
        {
          "if": {
            "properties": {
              "role": {
                "const": "other"
              }
            },
            "required": [
              "role"
            ]
          },
          "then": {
            "required": [
              "role_uri"
            ]
          }
        }
      ],
      "properties": {
        "activity_id": {
          "$ref": "#/$defs/urnUuid"
        },
        "id": {
          "$ref": "#/$defs/urnUuid"
        },
        "role": {
          "$ref": "#/$defs/relationRole"
        },
        "role_uri": {
          "$ref": "#/$defs/absoluteUri"
        },
        "state": {
          "$ref": "#/$defs/stateReference"
        },
        "type": {
          "const": "usage"
        }
      },
      "required": [
        "id",
        "type",
        "activity_id",
        "state",
        "role"
      ],
      "type": "object"
    },
    "utcDateTime": {
      "description": "RFC 3339 date-time normalized to UTC with uppercase Z and at most nanosecond precision.",
      "format": "date-time",
      "pattern": "^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(?:\\.[0-9]{1,9})?Z$",
      "type": "string"
    }
  },
  "$id": "https://nashspence.github.io/riverhog/v1/provenance/journal-entry.schema.json",
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "additionalProperties": false,
  "allOf": [
    {
      "if": {
        "properties": {
          "entry_kind": {
            "const": "journal_init"
          }
        },
        "required": [
          "entry_kind"
        ]
      },
      "then": {
        "not": {
          "required": [
            "previous_entry"
          ]
        },
        "properties": {
          "body": {
            "$ref": "#/$defs/journalInitBody"
          },
          "sequence": {
            "const": 0
          }
        }
      }
    },
    {
      "if": {
        "properties": {
          "entry_kind": {
            "const": "assertion"
          }
        },
        "required": [
          "entry_kind"
        ]
      },
      "then": {
        "properties": {
          "body": {
            "$ref": "#/$defs/assertionBody"
          },
          "sequence": {
            "minimum": 1
          }
        },
        "required": [
          "previous_entry"
        ]
      }
    },
    {
      "if": {
        "properties": {
          "entry_kind": {
            "const": "correction"
          }
        },
        "required": [
          "entry_kind"
        ]
      },
      "then": {
        "properties": {
          "body": {
            "$ref": "#/$defs/correctionBody"
          },
          "sequence": {
            "minimum": 1
          }
        },
        "required": [
          "previous_entry"
        ]
      }
    },
    {
      "if": {
        "properties": {
          "entry_kind": {
            "const": "checkpoint"
          }
        },
        "required": [
          "entry_kind"
        ]
      },
      "then": {
        "properties": {
          "body": {
            "$ref": "#/$defs/checkpointBody"
          },
          "sequence": {
            "minimum": 1
          }
        },
        "required": [
          "previous_entry"
        ]
      }
    }
  ],
  "description": "One immutable entry in a hash-chained RFC 7464 per-file provenance journal.",
  "properties": {
    "$schema": {
      "const": "https://nashspence.github.io/riverhog/v1/provenance/journal-entry.schema.json"
    },
    "body": {
      "type": "object"
    },
    "entry_kind": {
      "enum": [
        "journal_init",
        "assertion",
        "correction",
        "checkpoint"
      ],
      "type": "string"
    },
    "id": {
      "$ref": "#/$defs/urnUuid"
    },
    "journal_id": {
      "$ref": "#/$defs/urnUuid"
    },
    "notes": {
      "items": {
        "$ref": "#/$defs/nonEmptyString"
      },
      "minItems": 1,
      "type": "array",
      "uniqueItems": true
    },
    "previous_entry": {
      "$ref": "#/$defs/entryReference"
    },
    "profile": {
      "const": "https://nashspence.github.io/riverhog/v1/provenance"
    },
    "recorded_at": {
      "$ref": "#/$defs/utcDateTime"
    },
    "recorded_by_agent_id": {
      "$ref": "#/$defs/urnUuid"
    },
    "recording_environment_id": {
      "$ref": "#/$defs/urnUuid"
    },
    "schema_version": {
      "const": "1.0.0"
    },
    "sequence": {
      "maximum": "9223372036854775807",
      "minimum": 0,
      "type": "integer"
    },
    "type": {
      "const": "riverhog_provenance_journal_entry"
    }
  },
  "required": [
    "$schema",
    "profile",
    "schema_version",
    "id",
    "type",
    "journal_id",
    "sequence",
    "recorded_at",
    "recorded_by_agent_id",
    "entry_kind",
    "body"
  ],
  "title": "Riverhog provenance v1 journal entry",
  "type": "object"
}
```

</details>
