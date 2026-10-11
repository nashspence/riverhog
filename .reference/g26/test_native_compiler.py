from __future__ import annotations

import hashlib
import pytest

from riverhog_archive_contracts import HistoryJournalAnchor, MemberHistoryRoot, MemberHistoryStore
from riverhog_core.canonical_discovery_relevance import member_relevance
from riverhog_provenance import append_correction, assertion_reference, validate_journal
from riverhog_provenance_contracts import ContractCatalog
from riverhog_protocol.collection_production_provenance import collection_production_contract
from tests.support.member_history import member_history_selection_fixture
from tests.unit.test_member_history_discovery import _members, _completion
from tests.unit.test_provenance_binding import _bound

from factored_index import FactorIndex
from native_compiler import NativeCompiler


def compare_native(context, catalog, members, corpus, roots):
    objects, selected_items, expected = {}, [], {}
    for member, binding, primary, *_ in members:
        selected, closure, stored = member_history_selection_fixture(member, binding, corpus, roots=roots)
        objects.update(stored); selected_items.append((member, binding, selected))
        with closure, member_relevance(member=member, binding=binding, primary=primary,
            corpus=corpus, delivery_context_id=context, history_binding=selected, closure=closure, catalog=catalog) as native:
            expected[str(member.artifact_id)] = {(prefix, assertion, scope) for journal, prefix, assertion in native for scope in native[(journal,prefix,assertion)]}
    with FactorIndex() as db:
        NativeCompiler(db, corpus, MemberHistoryStore(lambda path:(objects[path],)), catalog).members(selected_items, context)
        db.seal(db.sizes(), {'archive': 'native-fixture', 'policy': 'g26-research'})
        for member, wanted in expected.items():
            actual = db.logical(member)
            assert actual == wanted, {'extra': actual-wanted, 'missing': wanted-actual}
        return db.sizes(), dict(db.counts), expected


@pytest.mark.parametrize('corrected', [False, True])
@pytest.mark.parametrize('extent', ['bound', 'retained'])
def test_shared_completion_exact_all_scope_parity(corrected, extent):
    context, catalog, members = _members()
    who, claim, raw = _completion(members[0][3], members[1][3])
    if corrected:
        original = validate_journal(raw)
        raw = append_correction(raw, (assertion_reference(original, claim['assertion_id']),), reason='withdraw', recorded_by_agent_id=who)
    completion = validate_journal(raw)
    corpus = {p.journal_id:p for _,_,p,_ in members}; corpus[completion.journal_id] = completion
    root = MemberHistoryRoot(HistoryJournalAnchor.from_mapping(completion.anchor), extent)
    sizes, counts, expected = compare_native(context, catalog, members, corpus, (root,))
    for i, (member,*_) in enumerate(members):
        key = completion.journal_sha256, claim['assertion_id'], 'member'
        assert (key in expected[str(member.artifact_id)]) == (i == 0 and not corrected and extent == 'bound')
        assert (completion.journal_sha256, claim['assertion_id'], 'recorded-history') in expected[str(member.artifact_id)]
    assert sizes['leaves'] < sum(map(len, expected.values()))


@pytest.mark.parametrize('hint', [None, ('a','b')])
def test_ordinary_native_member_matches_exact_provenance(hint):
    member, binding, primary, context, _ = _bound(hint=hint)
    compare_native(context, ContractCatalog((collection_production_contract(),)),
        [(member,binding,primary)], {primary.journal_id:primary}, ())


def test_exact_prefix_state_does_not_use_latest_head():
    context, catalog, members = _members()
    who, claim, raw = _completion(members[0][3], members[1][3])
    initial = validate_journal(raw)
    corrected = validate_journal(append_correction(raw, (assertion_reference(initial, claim['assertion_id']),), reason='withdraw', recorded_by_agent_id=who))
    corpus = {p.journal_id:p for _,_,p,_ in members}; corpus[corrected.journal_id] = corrected
    roots = (MemberHistoryRoot(HistoryJournalAnchor.from_mapping(initial.anchor),'bound'), MemberHistoryRoot(HistoryJournalAnchor.from_mapping(corrected.anchor),'retained'))
    _, _, expected = compare_native(context,catalog,members,corpus,roots)
    first = expected[str(members[0][0].artifact_id)]
    assert (initial.journal_sha256, claim['assertion_id'], 'member') in first
    assert (corrected.journal_sha256, claim['assertion_id'], 'member') not in first
    assert (corrected.journal_sha256, claim['assertion_id'], 'recorded-history') in first


def source_proof(binding, context):
    from riverhog_archive_contracts import ArchiveProvenanceIdentity, CollectionArchiveManifest, CollectionArtifactSetIdentity, ProvenanceRootDocument, ProvenanceRootIdentity, SourceMemberHistoryBindingProof, binding_tree_commitment
    root = ProvenanceRootDocument(archive_generation='1'*64,artifact_set_sha256='2'*64,
        delivery_context_id=context,binding_count=1,binding_tree_sha256=binding_tree_commitment((binding,)).root_sha256,journal_count=2,ordered_volume_sha256='3'*64)
    archive = CollectionArchiveManifest(archive_generation=root.archive_generation,
        artifact_set=CollectionArtifactSetIdentity(1,int(binding.bytes),root.artifact_set_sha256),ordered_volume_sha256='4'*64,
        provenance=ArchiveProvenanceIdentity(root.identity,ProvenanceRootIdentity(id='provenance-root',kind='provenance-root',path='provenance/root.json.age',plaintext_bytes=len(root.to_json_bytes()),sha256=root.identity,stored_bytes=1234,stored_sha256='0'*64)))
    return SourceMemberHistoryBindingProof('a'*64,1,archive.to_json_bytes(),root.to_json_bytes(),binding,0,())


@pytest.mark.parametrize('causal', [False,True])
@pytest.mark.parametrize('extent', ['bound-and-required-history','complete-retained-history'])
def test_late_input_claim_requires_import_and_causal_applicability(causal,extent):
    from riverhog_archive_contracts import MemberHistoryBuilder, MemberHistoryPrimary, MemberHistoryImport, provenance_structure_identity
    from riverhog_protocol import CollectionArtifactProvenanceBindingDocument
    from riverhog_protocol.collection_production_provenance import COLLECTION_MEMBER_ROLE
    from riverhog_provenance import append_assertions, assertion, MemberHistoryClosure
    context,catalog,members = _members()
    m0,b0,p0,state0 = members[0]; m1,b1,p1,_ = members[1]
    _,claim,raw = _completion(state0,state0)
    completion = validate_journal(raw); corpus = {p.journal_id:p for p in (p0,p1,completion)}
    sb,sc,objects = member_history_selection_fixture(m0,b0,corpus,roots=(MemberHistoryRoot(HistoryJournalAnchor.from_mapping(completion.anchor),'bound'),))
    sc.__exit__(None,None,None)
    proof = source_proof(sb,context)
    objects[provenance_structure_identity(proof.to_json_bytes()).relative_path] = proof.to_json_bytes()
    imported = MemberHistoryImport(proof.source_identity,proof.collection_id,hashlib.sha256(proof.archive_root).hexdigest(),'2'*64,sb.artifact_id,sb.history_sha256,proof.identity,extent,state0)
    if causal:
        recorder = p1.graph['agents'][0]['id']; out_state = p1.graph_validation.objects[b1.delivery_association_id]['state']
        edge = assertion('derivation',recorder,generated_state=out_state,used_state=state0,kind='transformation')
        p1 = validate_journal(append_assertions(b''.join(f.encoded for f in p1.frames),{'relations':[edge]},recorded_by_agent_id=recorder,catalog=catalog),catalog=catalog)
        b1 = CollectionArtifactProvenanceBindingDocument.model_validate({'artifact_id':str(m1.artifact_id),'journal':p1.anchor,'delivery_association_id':b1.delivery_association_id})
        corpus[p1.journal_id] = p1
    with MemberHistoryBuilder(artifact_id=str(m1.artifact_id),bytes=int(m1.bytes),sha256=m1.sha256,
        primary=MemberHistoryPrimary.from_mapping({'journal':b1.journal.model_dump(mode='json'),'delivery_association_id':b1.delivery_association_id})) as builder:
        builder.add_import(imported); output_binding,_ = builder.seal()
        objects.update({provenance_structure_identity(raw).relative_path:raw for raw in builder.objects()})
    store = MemberHistoryStore(lambda path:(objects[path],))
    with MemberHistoryClosure(store,lambda journal,size:(f.encoded for f in corpus[journal].frames),member_role=COLLECTION_MEMBER_ROLE,catalog=catalog) as closure, member_relevance(member=m1,binding=b1,primary=p1,corpus=corpus,delivery_context_id=context,history_binding=output_binding,closure=closure,catalog=catalog) as native:
        expected = {(prefix,aid,scope) for journal,prefix,aid in native for scope in native[(journal,prefix,aid)]}
    with FactorIndex() as db:
        NativeCompiler(db,corpus,store,catalog).members([(m1,b1,output_binding)],context)
        db.seal(db.sizes(),{'source':'test-import'})
        actual = db.logical(str(m1.artifact_id))
        assert actual == expected, {'extra':actual-expected,'missing':expected-actual}
        assert ((completion.journal_sha256,claim['assertion_id'],'input-history') in actual) == causal
