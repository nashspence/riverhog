"""Small-fixture lowering from actual Riverhog journals into the G26 model.

This is a DIFFERENTIAL TEST adapter, not a production source reader. It intentionally
materializes snapshot views and retains Python lookup maps. The production design
uses source-normal intervals and virtual prefix-parametric joins instead. It must
not be copied to main as proof of bounded-memory or all-prefix linear construction.
"""
from __future__ import annotations

import hashlib
from collections import defaultdict, deque

from riverhog_archive_contracts import BOUND_HISTORY_EXTENT, RETAINED_HISTORY_EXTENT
from riverhog_canonical_json import canonical_json_bytes
from riverhog_core.provenance_binding import verify_member_binding
from riverhog_provenance import external_reference, validate_journal_chunks
from riverhog_provenance.history import _external_references  # Research only; neutral owner.

from factored_index import FactorIndex


class NativeCompiler:
    def __init__(self, index: FactorIndex, corpus, store, catalog):
        self.index, self.corpus, self.store, self.catalog = index, corpus, store, catalog
        self.queue = deque()
        self.queued = set()
        self.summary, self.anchor, self.histories, self.by_entry = {}, {}, {}, {}
        self.raw_by_target, self.raw_by_state = defaultdict(list), defaultdict(list)
        self.raw_by_id, self.raw_by_artifact = defaultdict(list), defaultdict(list)
        self.effective_by_state, self.states_of_artifact = defaultdict(list), defaultdict(list)
        self.bound_extensions = defaultdict(list)
        for full in corpus.values():
            journal = full.journal_id
            index.exact('journals', (journal, full.journal_sha256), ('id',))
            retired = {}
            for sequence, frame in enumerate(full.frames):
                if frame.document['entry_kind'] == 'correction':
                    for item in frame.document['body']['retracts']:
                        retired[item['assertion_id']] = sequence
            digest, size = hashlib.sha256(), 0
            for sequence, frame in enumerate(full.frames):
                digest.update(frame.encoded); size += len(frame.encoded)
                self.by_entry[(journal, frame.document['id'])] = {
                    'journal_id': journal, 'through': frame.reference,
                    'prefix_sha256': digest.hexdigest(), 'prefix_bytes': str(size),
                }
                for rows in frame.document['body'].get('assertions', {}).values():
                    for row in rows:
                        index.exact('assertions', (journal, row['assertion_id'], sequence,
                            retired.get(row['assertion_id']), canonical_json_bytes(row)), ('journal', 'assertion'))
            index.counts['native_journal_scans'] += 1

    def snapshot(self, anchor):
        key = anchor['prefix_sha256']
        if key not in self.summary:
            full = self.corpus[anchor['journal_id']]
            summary = full if full.anchor == anchor else validate_journal_chunks(
                (f.encoded for f in full.frames[:int(anchor['through']['sequence'])+1]),
                catalog=self.catalog, require_profiles=False, expected_anchor=anchor,
                require_exact_tail=True,
            )
            self.summary[key], self.anchor[key] = summary, anchor
            self.index.exact('snapshots', (key, summary.journal_id,
                int(anchor['through']['sequence']), canonical_json_bytes(anchor)), ('id',))
            objects = summary.graph_validation.objects
            for row in objects.values():
                state = row.get('generated_state') if row['type'] == 'derivation' else row.get('state')
                if row['type'] in ('generation', 'invalidation', 'derivation') and state and state['scope'] == 'local':
                    self.effective_by_state[(key, state['object_id'])].append(row)
                if row['type'] == 'state':
                    self.states_of_artifact[(key, objects[row['occurrence_id']]['artifact_id'])].append(row['id'])
            for frame in summary.frames:
                for rows in frame.document['body'].get('assertions', {}).values():
                    for row in rows:
                        self.raw_by_id[(key, row['id'])].append(row)
                        target = row.get('subject', row.get('target'))
                        if isinstance(target, dict) and target.get('scope') == 'local':
                            self.raw_by_target[(key, target['object_id'])].append(row)
                        if row['type'] in ('observation', 'reported_description') and row['state']['scope'] == 'local':
                            self.raw_by_state[(key, row['state']['object_id'])].append(row)
                        if row['type'] == 'journal_subject' and row.get('artifact', {}).get('scope') == 'local':
                            self.raw_by_artifact[(key, row['artifact']['object_id'])].append(row)
                        if row['type'] == 'extension' and row['assertion_id'] not in summary.retracted_assertion_ids:
                            subject = row['subject']
                            exact = subject if subject['scope'] == 'external' else external_reference(summary, subject['object_id'])
                            self.bound_extensions[(key, canonical_json_bytes(exact))].append(row)
            self.index.counts['native_snapshot_views'] += 1
        return key

    def node(self, kind, *parts):
        parts = tuple(str(x) if isinstance(x, str) else x for x in parts)
        spec = (kind, *parts)
        node = self.index.node(kind, *parts)
        if spec not in self.queued:
            self.queued.add(spec); self.queue.append((node, spec))
        return node

    def emit(self, node, snapshot, row, scope):
        self.index.emit(node, snapshot, row['assertion_id'], scope)

    def link(self, node, kind, *parts):
        self.index.edge(node, self.node(kind, *parts))

    def history(self, binding):
        key = binding.history_sha256
        if key not in self.histories:
            doc = self.store.descriptor(binding)
            snapshot = self.snapshot(doc.primary.journal.to_mapping())
            state = self.summary[snapshot].graph_validation.objects[doc.primary.delivery_association_id]['state']['object_id']
            self.histories[key] = binding, doc, snapshot, state
        return key

    def import_history(self, imported):
        proof = self.store.source_proof(imported)
        key = self.history(proof.binding)
        _, _, snapshot, state = self.histories[key]
        if external_reference(self.summary[snapshot], state) != imported.input_state:
            raise ValueError('import source state mismatch')
        return key

    def members(self, items, context):
        for member, binding, selected in items:
            history = self.history(selected)
            _, doc, snapshot, state = self.histories[history]
            summary = self.summary[snapshot]
            verify_member_binding(member=member, binding=binding, summary=summary, delivery_context_id=context)
            if (doc.artifact_id, doc.bytes, doc.sha256) != (member.artifact_id, member.bytes, member.sha256) or doc.primary.to_mapping() != {
                'journal': binding.journal.model_dump(mode='json'), 'delivery_association_id': binding.delivery_association_id,
            }:
                raise ValueError('member history differs')
            root = self.node('member', str(member.artifact_id))
            self.index.root(str(member.artifact_id), {'binding': selected.to_mapping(), 'context': context}, [root])
            self.emit(root, snapshot, summary.graph_validation.objects[binding.delivery_association_id], 'member')
            self.link(root, 'state', snapshot, state, 'member')
            self.link(root, 'collection', snapshot, context)
            self.link(root, 'history-doc', history, RETAINED_HISTORY_EXTENT)
            self.link(root, 'late', history, 'member')
            artifact = summary.graph_validation.objects[summary.graph_validation.objects[state]['occurrence_id']]['artifact_id']
            self.link(root, 'continuity', snapshot, artifact)
            for imported in self.store.imports(selected):
                self.link(root, 'import-visit', self.import_history(imported))
        self.drain()

    def drain(self):
        while self.queue:
            node, spec = self.queue.popleft()
            kind, *args = spec
            self.index.counts['native_selector_compiles'] += 1
            if kind in ('member', 'causal'):
                continue
            if kind == 'snapshot-doc':
                snapshot = args[0]; self.index.leaf(node, snapshot)
                anchor = self.anchor[snapshot]
                self.link(node, 'prefix-deps', anchor['journal_id'], int(anchor['through']['sequence']))
            elif kind == 'prefix-deps':
                journal, sequence = args
                if sequence: self.link(node, 'prefix-deps', journal, sequence-1)
                frame = self.corpus[journal].frames[sequence]
                for ref in _external_references(frame.document['body'].get('assertions', {})):
                    anchor = self.by_entry[(ref['journal_id'], ref['entry']['entry_id'])]
                    target = self.snapshot(anchor)
                    if external_reference(self.summary[target], ref['object_id']) != ref:
                        raise ValueError('foreign documentary endpoint differs')
                    self.link(node, 'snapshot-doc', target)
                parent = frame.document['body'].get('journal', {}).get('forked_from')
                if parent is not None: self.link(node, 'snapshot-doc', self.snapshot(parent))
            elif kind == 'history-doc':
                history, extent = args; binding, _, _, _ = self.histories[history]
                for root in self.store.roots(binding, extent=extent):
                    self.link(node, 'snapshot-doc', self.snapshot(root.journal.to_mapping()))
                for imported in self.store.imports(binding):
                    self.link(node, 'history-doc', self.import_history(imported), imported.extent)
            elif kind == 'import-visit':
                history = args[0]; binding, _, snapshot, state = self.histories[history]
                causal = self.node('causal', canonical_json_bytes(external_reference(self.summary[snapshot], state)).hex())
                self.index.gate(node, causal, self.node('late', history, 'input-history'))
                for imported in self.store.imports(binding):
                    self.link(node, 'import-visit', self.import_history(imported))
            elif kind == 'late':
                history, scope = args; binding, _, snapshot, state = self.histories[history]
                summary = self.summary[snapshot]; objects = summary.graph_validation.objects
                occurrence = objects[state]['occurrence_id']
                endpoints = [state, occurrence, objects[occurrence]['artifact_id']] + [
                    r['activity_id'] for r in self.effective_by_state[(snapshot, state)] if r['type'] == 'generation'
                ]
                for root in self.store.roots(binding, extent=BOUND_HISTORY_EXTENT):
                    target = self.snapshot(root.journal.to_mapping())
                    for endpoint in endpoints:
                        self.link(node, 'bound-endpoint', target,
                            canonical_json_bytes(external_reference(summary, endpoint)).hex(), scope)
            elif kind == 'bound-endpoint':
                snapshot, endpoint, scope = args
                for row in self.bound_extensions[(snapshot, bytes.fromhex(endpoint))]:
                    self.emit(node, snapshot, row, scope)
            elif kind == 'continuity':
                snapshot, artifact = args
                for state in self.states_of_artifact[(snapshot, artifact)]:
                    self.link(node, 'state', snapshot, state, 'recorded-history')
            elif kind == 'collection':
                snapshot, context = args; objects = self.summary[snapshot].graph_validation.objects
                if objects.get(context) is not None: self.emit(node, snapshot, objects[context], 'collection')
                for row in self.raw_by_target[(snapshot, context)]:
                    if row['type'] == 'extension': self.emit(node, snapshot, row, 'collection')
            elif kind == 'direct':
                snapshot, target, scope = args; row = self.summary[snapshot].graph_validation.objects.get(target)
                if row is not None: self.emit(node, snapshot, row, scope)
            elif kind == 'own':
                snapshot, target, scope = args
                rows = (*self.raw_by_id[(snapshot, target)], *self.raw_by_target[(snapshot, target)], *self.raw_by_artifact[(snapshot, target)])
                for row in rows:
                    self.emit(node, snapshot, row, scope)
                    if row['type'] in ('observation', 'reported_description') and 'source_context_id' in row:
                        self.link(node, 'direct', snapshot, row['source_context_id'], scope)
            elif kind == 'observed':
                snapshot, state, scope = args
                for row in self.raw_by_state[(snapshot, state)]:
                    self.emit(node, snapshot, row, scope)
                    if 'source_context_id' in row:
                        self.link(node, 'direct', snapshot, row['source_context_id'], scope)
            elif kind == 'activity':
                snapshot, activity, scope = args; objects = self.summary[snapshot].graph_validation.objects
                if objects.get(activity) is not None:
                    self.link(node, 'own', snapshot, activity, scope)
                    for context in objects[activity].get('contexts', ()):
                        self.link(node, 'direct', snapshot, context['context_id'], scope)
            elif kind == 'state':
                snapshot, state, scope = args; summary = self.summary[snapshot]; objects = summary.graph_validation.objects
                occurrence = objects[objects[state]['occurrence_id']]
                for target in (state, occurrence['id'], occurrence['artifact_id']):
                    self.link(node, 'own', snapshot, target, scope)
                self.link(node, 'observed', snapshot, state, scope)
                if 'source_context_id' in occurrence:
                    self.link(node, 'direct', snapshot, occurrence['source_context_id'], scope)
                if scope in ('member', 'input-history'):
                    self.link(node, 'causal', canonical_json_bytes(external_reference(summary, state)).hex())
                for relation in self.effective_by_state[(snapshot, state)]:
                    self.emit(node, snapshot, relation, scope)
                    if 'activity_id' in relation:
                        self.link(node, 'activity', snapshot, relation['activity_id'], scope)
                    if relation['type'] == 'derivation':
                        for field in ('usage_id', 'generation_id'):
                            if field in relation: self.link(node, 'direct', snapshot, relation[field], scope)
                        ref = relation['used_state']
                        target = snapshot if ref['scope'] == 'local' else self.snapshot(self.by_entry[(ref['journal_id'], ref['entry']['entry_id'])])
                        self.link(node, 'state', target, ref['object_id'], 'input-history')
            else:
                raise ValueError('unknown fixed selector: ' + kind)
