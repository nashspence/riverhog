# Primary sources and implementation consequences

Reviewed 2026-10-03. Use the repository's locked versions when implementing; current
web documentation is not permission to upgrade dependencies as incidental work.

- [#903](https://github.com/nashspence/riverhog/issues/903) establishes this branch's
  non-authoritative status, audited-base requirement and exact handoff identity.
- [#950](https://github.com/nashspence/riverhog/issues/950) supplies generation,
  publication and history boundaries. [#895](https://github.com/nashspence/riverhog/issues/895)
  separates promises from documentation and explicitly permits Markdown authoring.
  [#440](https://github.com/nashspence/riverhog/issues/440) retains real documentation
  journeys; [#444](https://github.com/nashspence/riverhog/issues/444) owns release delivery.
  #952 records the selected refinement; it is not inferred solely from older plans.

## Native documentation and metadata

[PyPA pyproject specification](https://packaging.python.org/en/latest/specifications/pyproject-toml/)
distinguishes the one-line project description (core Summary), long readme Description,
and static versus dynamically supplied metadata. Backends must respect declared
static scalar values. The design therefore prefers a new, reproducibly prepared
source tree before the backend runs, or an explicitly supported dynamic input. It
does not propose post-build wheel metadata editing. Source archives must retain all
selected data to reproduce the wheel without the documentation branch.

[Click help documentation](https://click.palletsprojects.com/en/stable/documentation/)
and [Python 3.12 argparse](https://docs.python.org/3.12/library/argparse.html) show that
native syntax and help prose are distinct inputs. Argparse expands format parameters
in argument help. Authored `%` must remain literal unless a small explicitly designed
source-owned mechanism supplies facts. Test actual help AND actual parsing, not a
JSON approximation. Framework suppression, flags, aliases and defaults remain source
semantics and cannot be changed by a prose entry.

[OpenAPI 3.1.1](https://spec.openapis.org/oas/v3.1.1.html) defines descriptions/summaries
at specific object locations and permits CommonMark in designated descriptions.
The schema object has a different structure from a user value with a property named
`description`. [FastAPI's documented OpenAPI generation hook](https://fastapi.tiangolo.com/how-to/extending-openapi/)
provides an annotation seam while preserving native route/schema generation. It is
not a reason to treat all annotations as harmless: prose may currently be the only
statement of a guarantee. That meaning needs a real authority before relocation.

[OCI annotations](https://specs.opencontainers.org/image-spec/annotations/) include
human descriptions alongside source/version/other metadata. Their destination in
an image does not imply that all human prose must originate on main. Project only
editorial fields; preserve operational and legal values, then verify the built image.

## Markdown

[CommonMark 0.31.2](https://spec.commonmark.org/0.31.2/) supplies a standard syntax,
including code blocks and raw HTML. It does not itself provide an application safety
policy or cross-file binding system. [markdown-it-py security guidance](https://markdown-it-py.readthedocs.io/en/latest/security.html)
warns about raw HTML and link handling; use a deliberate safe configuration and
validate the parsed link targets. Never substitute regex Markdown parsing. The
prototype uses markdown-it-py 4.2.0, the version present in the audited `uv.lock`.

## Repository observations

The audited `documentation.py` permits only inline explanation/guide text and one
JSON file per version. `cli_documentation.py` harvests parser prose into its bound
record. `generation.py::prepared_source` admits version/lockfile preparation, and
`release.py::build_release_evidence` builds artifacts from that prepared tree.
These are the actual migration seams, not settled format requirements.

The previously retained Closure bytes with SHA-256
`1c17175e4802b79a57a6f6ae3e269e42670241701667ac6483fdc94295592208`
contain 4,747 root elements across the 23 current interface families, including
3,300 Python roots and 224 CLI roots. This is an archived-artifact observation, NOT
fresh generation of the audited main, and it does not count all documentation member
obligations. The producer must traverse actual named members; a root count alone
cannot establish coverage. No generated Closure copy is added to this branch.

## Front-matter and review refinement

[PyYAML's documented token, node and loader APIs](https://pyyaml.org/wiki/PyYAMLDocumentation)
permit parsing metadata without constructing arbitrary Python objects. BaseLoader
keeps scalar values as strings; that alone does not reject aliases, duplicate keys,
merge keys or unknown application fields. The revision layers explicit token/schema
checks and byte/nesting budgets over those APIs. It uses installed PyYAML 6.0.3 in the
isolated witness environment; integration must reconcile the native dependency lock.

The Markdown security documentation above also warns against uncontrolled generated
DOM names. This revision prefixes emitted heading IDs while preserving logical
Markdown fragment names in authoring. Front matter is excluded from body rendering.

The central index and committed review stamp were a design choice, not a requirement
of Markdown, Git or the native metadata specifications. The updated owning issue
chooses co-located front matter and generated candidate evidence instead. Trust comes
from observing the prepared artifact surfaces, human review, existing offline/protected
approval, and promotion of those exact bytes. No parser or hashing API proves prose
accuracy; there is no claim that the reference implements full release attestation.
