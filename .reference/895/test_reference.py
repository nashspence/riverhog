"""Focused reference tests. Does not import or execute the Riverhog runtime."""
from __future__ import annotations

import copy
import hashlib
import json
import tempfile
import unittest
from html.parser import HTMLParser
from pathlib import Path
from urllib.parse import unquote, urlsplit

from legacy import from_legacy
from records import BoundaryError, at, build, canonical_json_bytes, digest, loads, pack, partition, restore, token, unpack, validate_closure, validate_pair
from render import build_pages, contract_body, element_file, emit, inventory_file, literal

FIXTURE = Path(__file__).with_name('fixture.json')


class Document(HTMLParser):
    def __init__(self, content):
        super().__init__(convert_charrefs=True)
        self.values = []
        self.links = []
        self.ids = []
        self.scripts = []
        self.current = None
        self.feed(content)

    def handle_starttag(self, tag, attrs):
        attrs = dict(attrs)
        if 'id' in attrs:
            self.ids.append(attrs['id'])
        if tag == 'a':
            self.links.append(attrs['href'])
        if tag == 'script':
            self.scripts.append(attrs)
        if tag == 'code' and 'data-literal' in attrs:
            self.current = [attrs['data-literal'], '']

    def handle_data(self, value):
        if self.current is not None:
            self.current[1] += value

    def handle_endtag(self, tag):
        if tag == 'code' and self.current is not None:
            self.values.append(tuple(self.current))
            self.current = None


def leaf_values(value, pointer=''):
    if isinstance(value, dict) and value:
        for key, item in sorted(value.items()):
            yield from leaf_values(item, pointer + '/' + token(key))
    elif isinstance(value, list) and value:
        for index, item in enumerate(value):
            yield from leaf_values(item, f'{pointer}/{index}')
    else:
        yield pointer, value


class ReferenceTests(unittest.TestCase):
    def setUp(self):
        self.fixture = loads(FIXTURE.read_bytes())
        self.source = self.fixture['source']
        self.tag = next(r for r in self.source['records'] if r['name'] == 'CollectionTag')
        self.function = next(r for r in self.source['records'] if r['name'].endswith('.access_covers'))

    def test_pinned_record_values_match_existing_jcs_hashes(self):
        by_name = {r['name']: r['value'] for r in self.source['records']}
        for record in self.fixture['verified_exact_owned_json']:
            self.assertEqual(hashlib.sha256(canonical_json_bytes(by_name[record['name']])).hexdigest(), record['exact_owned_json_sha256'])

    def test_reproducibility_and_input_order(self):
        c, a = build(self.source)
        other = copy.deepcopy(self.source)
        other['records'].reverse()
        other['extent_analysis'].reverse()
        self.assertEqual((c, a), build(other))
        self.assertEqual(build_pages(c,a), build_pages(*build(other)))

    def test_partition_reconstructs_every_selected_record(self):
        c, a = build(self.source)
        records, data = validate_closure(c), validate_pair(c,a)
        for original in self.source['records']:
            identity = original['id']
            self.assertEqual(restore(records[identity]['value'],data['overlays'][identity]['moved_fields']), original['value'])
        self.assertNotIn('reason',records[self.tag['id']]['value']['x-riverhog-extent'])
        rule=next(r for r in records.values() if r['interface']=='extent')
        self.assertNotIn('requirement',rule['value'])

    def test_standalone_closure_does_not_need_audit(self):
        c,a=build(self.source)
        self.assertEqual(len(validate_closure(c)),15)
        pages=build_pages(c,None)
        self.assertNotIn('audit.json',pages)
        self.assertIn(b'Audit Record not supplied',pages['index.html'])
        self.assertEqual(c['records'],loads(canonical_json_bytes(c))['records'])

    def test_audit_edits_do_not_change_contract(self):
        before,_=build(self.source)
        self.source['source_revision']='1'*40
        self.tag['value']['x-riverhog-extent']['reason']='changed explanatory rationale'
        self.tag['sources'][0]['source']['line']=123
        self.source['discovery_accounting']['selected_elements']=999
        self.source['extent_analysis'][0]['reason']='legacy classification text changed'
        after,audit=build(self.source)
        self.assertEqual(before,after)
        self.assertEqual(audit['closure_sha256'],digest(before))

    def test_declaration_change_invalidates_old_audit_binding(self):
        c,a=build(self.source)
        self.tag['value']['maxLength']=512
        self.source['extent_analysis'][0]['maximum']=512
        changed,_=build(self.source)
        self.assertNotEqual(digest(c),digest(changed))
        with self.assertRaises(BoundaryError):validate_pair(changed,a)

    def test_stale_copied_extent_bound_rejected(self):
        self.source['extent_analysis'][0]['maximum']=17
        with self.assertRaises(BoundaryError):build(self.source)

    def test_audit_cannot_overwrite_a_normative_field(self):
        c,a=build(self.source)
        data=unpack(a['data'])
        data['overlays'][self.tag['id']]['moved_fields'].append({'pointer':'/maxLength','role':'rationale','value':1})
        a['data']=pack(data)
        with self.assertRaises(BoundaryError):validate_pair(c,a)

    def test_standalone_rejects_audit_field_leak(self):
        c,_=build(self.source)
        records=unpack(c['records'])
        records[self.tag['id']]['value']['description']='audit-only annotation'
        c['records']=pack(records)
        with self.assertRaises(BoundaryError):validate_closure(c)

    def test_unknown_roles_and_extent_policies_fail_closed(self):
        with self.assertRaises(BoundaryError):partition('http-schemas',{'type':'string','x-new-proof':'not classified'})
        with self.assertRaises(BoundaryError):partition('http-schemas',{'type':'array','maxItems':10,'x-riverhog-extent':{'policy':'segmented_no_total_max','reason':'scope needs review'}})
        self.function['value']['unreviewed_authority']='not guessed'
        with self.assertRaises(BoundaryError):build(self.source)

    def test_schema_annotation_vs_property_or_instance_data(self):
        value={'type':'object','description':'schema annotation','properties':{'description':{'type':'string','description':'field annotation'}},'required':['description'],'default':{'description':'actual instance data','$ref':'not a schema reference'},'enum':[{'examples':[]}], 'additionalProperties':False}
        norm,moved=partition('http-schemas',value)
        self.assertEqual(norm['default'],value['default'])
        self.assertEqual(norm['enum'],value['enum'])
        self.assertIn('description',norm['properties'])
        self.assertEqual(restore(norm,moved),value)
        self.assertEqual({m['pointer'] for m in moved},{'/description','/properties/description/description'})

    def test_boolean_schema_is_not_an_object_or_an_absent_type(self):
        self.tag["value"] = False
        self.source["extent_analysis"] = []
        c, a = build(self.source)
        pages = build_pages(c, a)
        self.assertIn("false", [v for _, v in Document(pages[inventory_file("riverhog", "http-schemas")].decode()).values])
        self.assertIn("false", [v for _, v in Document(contract_body(self.tag["id"], validate_closure(c)[self.tag["id"]], validate_closure(c))).values])

    def test_absence_is_not_a_total_size_promise(self):
        value={'type':'array','items':{'type':'number'}}
        norm,moved=partition('http-schemas',value)
        self.assertEqual(norm,value)
        self.assertEqual(moved,[])
        self.assertNotIn('semantic_maximum',norm)

    def test_exact_integers_and_string_values_remain_distinct(self):
        value={'large':(1<<256)-1,'string':str((1<<256)-1),'zero':0,'false':False,'nothing':None,'empty':[],'map':{},'path/a~b':[-(1<<80)]}
        self.assertEqual(unpack(pack(value)),value)
        self.assertEqual(type(unpack(pack(value))['large']),int)
        self.assertEqual(type(unpack(pack(value))['string']),str)
        self.assertEqual(pack(value),loads(canonical_json_bytes(pack(value))))
        for bad in [{'value':'01','integer_paths':['']},{'value':'9007199254740993','integer_paths':['','']},{'value':'1','integer_paths':['']}]:
            with self.assertRaises(BoundaryError):unpack(bad)

    def test_duplicate_keys_and_nonfinite_input_rejected(self):
        for raw in [b'{"a":1,"a":2}',b'{"a":NaN}',b'{"a":Infinity}',b'{"a":9007199254740993}']:
            with self.assertRaises(ValueError):loads(raw)
        with self.assertRaises(ValueError):pack(float('nan'))

    def test_overlapping_ownership_and_unbound_analysis_rejected(self):
        duplicate=copy.deepcopy(self.tag);duplicate['id']='duplicate-address'
        self.source['records'].append(duplicate)
        with self.assertRaises(BoundaryError):build(self.source)
        self.source['records'].pop()
        self.source['extent_analysis'][0]['source_pointer']='/not-owned'
        with self.assertRaises(BoundaryError):build(self.source)

    def test_audit_pointer_cannot_be_rerouted(self):
        c,a=build(self.source)
        data=unpack(a['data'])
        data['overlays'][self.tag['id']]['source_pointer']='/different-subject'
        a['data']=pack(data)
        with self.assertRaises(BoundaryError):validate_pair(c,a)

    def test_witnesses_do_not_create_contract_guarantees(self):
        c,_=build(self.source)
        self.source['witnesses']=[{'id':'synthetic-test-only','analysis_ids':[self.source['extent_analysis'][0]['id']]*2,'record':{'unestablished_claims':['restart'],'test_node_ids':['fixture:test-not-run'],'scope':'synthetic unit-test context, not repository evidence'}}]
        c2,a=build(self.source)
        self.assertEqual(c,c2)
        self.assertEqual(len(unpack(a['data'])['witnesses'][0]['analysis_ids']),1)
        pages=build_pages(c2,a)
        self.assertIn(b'Recorded unestablished audit claims',pages[inventory_file('riverhog','http-schemas')])
        self.assertNotIn(b'Recorded unestablished audit claims',pages[inventory_file('release','compatibility-guarantees')])
        self.source['witnesses'][0]['analysis_ids']=['missing']
        with self.assertRaises(BoundaryError):build(self.source)

    def test_declared_module_member_nesting(self):
        parent=next(r for r in self.source['records'] if r['name'].endswith('.ApplicationAccess'))
        member=copy.deepcopy(parent)
        member.update(id='synthetic-member',name=parent['name']+'.method',pointer=parent['pointer']+'.method')
        member['value'].update(name='method',unit='member',owner=parent['name'],contract={'kind':'method','signature':'(self) -> None'})
        self.source['records'].append(member)
        c,a=build(self.source)
        page=build_pages(c,a)[inventory_file('riverhog-application-access','python')].decode()
        self.assertIn('class="member"',page)
        self.assertLess(page.index('>'+parent['name']+'</a>'),page.index('>'+member['name']+'</a>'))
        member['value']['owner']='unrelated.name.with.dots'
        with self.assertRaises(BoundaryError):build(self.source)

    def test_inventories_show_real_values_and_mutations(self):
        c,a=build(self.source)
        pages=build_pages(c,a)
        page=Document(pages[inventory_file('riverhog-application-access','python')].decode())
        self.assertEqual({v for _,v in page.values},{'class','function','type-alias'})
        compat=pages[inventory_file('release','compatibility-guarantees')].decode()
        for record in self.source['records']:
            if record['interface']=='compatibility-guarantees':self.assertIn(record['value'],[v for _,v in Document(compat).values])
        self.function['value']['contract']['kind']='recorded-new-kind'
        promise=next(r for r in self.source['records'] if r['interface']=='compatibility-guarantees')
        promise['value']='changed promise, not a classification'
        pages=build_pages(*build(self.source))
        self.assertIn('recorded-new-kind',[v for _,v in Document(pages[inventory_file('riverhog-application-access','python')].decode()).values])
        self.assertIn('changed promise, not a classification',[v for _,v in Document(pages[inventory_file('release','compatibility-guarantees')].decode()).values])

    def test_every_normative_leaf_is_primary_visible(self):
        c,a=build(self.source)
        records=validate_closure(c)
        for identity, record in records.items():
            actual=Document(contract_body(identity,record,records)).values
            expected=[(path,Document(literal(value,path)).values[0][1]) for path,value in leaf_values(record['value'])]
            self.assertEqual(sorted(actual),sorted(expected))
        pages=build_pages(c,a)
        self.assertEqual(len([p for p in pages if p.startswith('e-')]),len(records))

    def test_hostile_and_difficult_literal_text(self):
        cases=['^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$', '\"(v: Annotated[str, Pattern(\'a](b\')]) -> None\"', '`a` ](not-a-link) <script>bad()</script> & &#40; "quotes"', 'combining e\u0301 versus é; 雪 😀', 'line1\r\nline2\tindent', '', '\x00\x01']
        for value in cases:
            parser=Document(literal(value))
            expected=json.dumps(value,ensure_ascii=False) if any(ord(c)<32 and c not in '\n\t\r' for c in value) else value
            self.assertEqual(parser.values,[('',expected)])
            self.assertEqual(parser.links,[])
            self.assertEqual(parser.scripts,[])

    def test_scoped_refs_resolve_without_audit_and_make_real_links(self):
        self.tag['value']={'type':'object','properties':{'tag':{'$ref':'#/components/schemas/Other'}}}
        self.source['extent_analysis'] = []  # The ref fixture replaces the old bounded schema.
        other=copy.deepcopy(self.tag);other.update(id='other-schema',name='Other',pointer=self.tag['pointer'].rsplit('/',1)[0]+'/Other',value={'type':'string'})
        self.source['records'].append(other)
        c,a=build(self.source)
        pages=build_pages(c,None)
        self.assertIn(element_file('other-schema')+'#v-',pages[element_file(self.tag['id'])].decode())
        self.source['records'].pop()
        with self.assertRaises(BoundaryError):build(self.source)

    def test_python_local_recursive_schema_ref_is_not_expanded_forever(self):
        self.function['value']['contract']['schema']={'$defs':{'Node':{'type':'object','properties':{'child':{'$ref':'#/$defs/Node'}}}},'$ref':'#/$defs/Node'}
        c,a=build(self.source)
        page=build_pages(c,None)[element_file(self.function['id'])].decode()
        self.assertIn('Definition</a>',page)
        self.assertLess(len(page),30000)

    def test_html_links_and_ids_not_regex_matched_literals(self):
        pages=build_pages(*build(self.source))
        parsed={name:Document(content.decode()) for name,content in pages.items() if name.endswith('.html')}
        for name,page in parsed.items():
            self.assertEqual(len(page.ids),len(set(page.ids)),name)
            for href in page.links:
                u=urlsplit(href)
                if u.scheme:continue
                target=u.path or name
                if target=='manifest.json':continue
                self.assertIn(target,pages)
                if u.fragment and target.endswith('.html'):self.assertIn(unquote(u.fragment),parsed[target].ids)

    def test_manifest_is_acyclic_and_output_protected(self):
        c,a=build(self.source)
        with tempfile.TemporaryDirectory() as temp:
            output=Path(temp)/'preview'
            emit(c,a,output)
            manifest=loads((output/'manifest.json').read_bytes())
            self.assertNotIn('manifest.json',manifest['files'])
            for path,sha in manifest['files'].items():self.assertEqual(hashlib.sha256((output/path).read_bytes()).hexdigest(),sha)
            with self.assertRaises(BoundaryError):emit(c,a,output)

    def test_legacy_adapter_reads_owned_values_and_ignores_old_map(self):
        projection={'external_contract':{'extents':{'decisions':self.source['extent_analysis']}}}
        elements=[];sources={}
        for record in self.source['records']:
            p=record['pointer'][1:].split('/')
            parent=projection
            for part in p[:-1]:parent=parent.setdefault(part.replace('~1','/').replace('~0','~'),{})
            parent[p[-1].replace('~1','/').replace('~0','~')]=copy.deepcopy(record['value'])
            elements.append({'id':record['id'],'authority':record['authority'],'interface':record['interface'],'pointers':[record['pointer']],'source_authority_ids':[s['id'] for s in record['sources']]})
            sources.update({s['id']:s for s in record['sources']})
        root={'format':'riverhog-contract-machine-closure/v1','series':'v1','projection':projection,'elements':elements,'sources':list(sources.values()),'trace':{'extent_sources':[],'segmented_extent_witnesses':[]},'atlas':{'relationships':{'contract_map':'poisoned'}}}
        source=from_legacy(root,self.source['source_revision'])
        c,_=build(source)
        root['atlas']={'made_up_family':'everything'}
        self.assertEqual(c,build(from_legacy(root,self.source['source_revision']))[0])
        self.assertEqual(set(validate_closure(c)),{r['id'] for r in self.source['records']})


if __name__=='__main__':
    unittest.main()
