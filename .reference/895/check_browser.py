"""Real Chromium checks for local generated pages, not a GitHub Pages acceptance.

Requires Playwright and a Chromium executable. No server/network access is used:
Playwright fulfills requests from the exact generated static artifact map.
"""
from __future__ import annotations

import argparse
import copy
import json
from pathlib import Path
from urllib.parse import unquote, urlsplit

from playwright.sync_api import sync_playwright

from records import build, loads, validate_closure
from render import build_pages, element_file, inventory_file, STYLE, MODES


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--chromium', default='/usr/bin/chromium')
    p.add_argument('--dom-only', action='store_true', help='Use set_content when navigation is blocked; URL/history tests are then NOT run')
    p.add_argument('--report', type=Path)
    p.add_argument('--screenshots', type=Path)
    args = p.parse_args()
    source = loads(Path(__file__).with_name('fixture.json').read_bytes())['source']
    closure, audit = build(source)
    files = build_pages(closure, audit)
    # Manifest is navigation metadata; build_pages leaves it to the artifact writer.
    files['manifest.json'] = b'{}'
    regex_id = next(k for k,r in validate_closure(closure).items() if r['name']=='gogurt_core.GOGURT_ROUTE_PATTERN')
    regex_path = element_file(regex_id)
    app_inventory = inventory_file('riverhog-application-access','python')
    compatibility = inventory_file('release','compatibility-guarantees')
    origin = 'https://reference.invalid/riverhog/preview/'
    outcomes = []

    def serve(route):
        path = unquote(urlsplit(route.request.url).path).removeprefix('/riverhog/preview/')
        if path not in files:
            route.fulfill(status=404,body='missing fixture path')
            return
        kind = 'text/html' if path.endswith('.html') else 'text/css' if path.endswith('.css') else 'text/javascript' if path.endswith('.js') else 'application/json'
        route.fulfill(status=200,content_type=kind,body=files[path])

    with sync_playwright() as pw:
        browser = pw.chromium.launch(executable_path=args.chromium, headless=True, args=['--no-sandbox'])
        context = browser.new_context(viewport={'width':1280,'height':900})
        context.route('**/*',serve)
        page = context.new_page()
        def visit(page, filename):
            if args.dom_only:
                html = files[filename.split('?')[0].split('#')[0]].decode()
                html = html.replace('<link rel="stylesheet" href="style.css">', '<style>' + STYLE + '</style>')
                html = html.replace('<script src="modes.js" defer></script>', '<script>' + MODES + '</script>')
                # Put the script after its controls, matching the original defer.
                html = html.replace('<script>' + MODES + '</script>', '')
                html = html.replace('</body>', '<script>' + MODES + '</script></body>')
                page.goto("about:blank")
                page.set_content(html)
            else:
                page.goto(origin + filename)
        page_errors=[]
        page.on('pageerror',lambda error:page_errors.append(str(error)))
        for width in (1280,640):
            page.set_viewport_size({'width':width,'height':900})
            for filename in ('index.html',app_inventory,compatibility,regex_path):
                visit(page,filename)
                assert page.evaluate('document.documentElement.scrollWidth <= innerWidth'),(width,filename)
                outcomes.append({'check':'layout','width':width,'page':filename,'result':'pass'})
        page.set_viewport_size({'width':1280,'height':900})
        visit(page,regex_path+'#contract')
        expected=next(r for r in source['records'] if r['id']==regex_id)['value']['contract']['value']
        assert page.locator('code[data-literal="/contract/value"]').inner_text()==expected
        body=page.locator('[data-contract]').inner_html()
        if args.dom_only:
            page.evaluate('displayMode(true)')
            assert page.locator('.audit-panel').is_visible()
            assert page.locator('[data-contract]').inner_html()==body
            page.evaluate('displayMode(false)')
            assert not page.locator('.audit-panel').is_visible()
            assert page.locator('[data-contract]').inner_html()==body
            outcomes.append({'check':'mode-display-function-and-body-invariance','result':'pass','not_tested':'URL transitions, history and click navigation'})
        else:
            page.locator('#audit-mode').check()
            assert 'audit=1' in page.url and page.url.endswith('#contract')
            assert page.locator('.audit-panel').is_visible()
            assert page.locator('[data-contract]').inner_html()==body
            page.locator('a[href^="i-"]').first.click()
            assert 'audit=1' in page.url
            page.go_back()
            assert page.locator('#audit-mode').is_checked()
            page.locator('#audit-mode').uncheck()
            assert page.locator('[data-contract]').inner_html()==body
            assert not page.locator('.audit-panel').is_visible()
            outcomes.append({'check':'audit-mode-navigation-and-body-invariance','result':'pass'})
        visit(page,app_inventory)
        kinds=page.locator('code[data-literal="/contract/kind"]').all_text_contents()
        assert set(kinds)=={'class','function','type-alias'},kinds
        visit(page,compatibility)
        visible=page.locator('tbody tr td:nth-child(2) code').all_text_contents()
        assert set(visible)=={r['value'] for r in source['records'] if r['interface']=='compatibility-guarantees'}
        outcomes.append({'check':'comparison-facts-from-source','result':'pass'})
        if args.screenshots:
            args.screenshots.mkdir(parents=True,exist_ok=True)
            page.screenshot(path=str(args.screenshots/'compatibility.png'),full_page=True)
            visit(page,regex_path+'?audit=1')
            page.evaluate('displayMode(true)')
            page.screenshot(path=str(args.screenshots/'literal-audit.png'),full_page=True)
        # Check changed and hostile inputs in the actual DOM, not just source tokens.
        cases=['`ticks` ](not-a-link) <script>window.bad=1</script> & &#40; "quoted"','e\u0301 / é · 雪 😀','line1\r\nline2\tindent','', (1<<256)-1]
        for value in cases:
            altered=copy.deepcopy(source)
            next(r for r in altered['records'] if r['id']==regex_id)['value']['contract']['value']=value
            files=build_pages(*build(altered));files['manifest.json']=b'{}'
            visit(page,regex_path)
            assert page.locator('code[data-literal="/contract/value"]').text_content()==str(value)
            assert page.evaluate('window.bad === undefined')
            assert page.locator('script').count()==1
        outcomes.append({'check':'difficult-literal-mutations-visible-in-chromium','cases':len(cases)+1,'result':'pass'})
        files=build_pages(closure,audit);files['manifest.json']=b'{}'
        nojs=browser.new_context(java_script_enabled=False,viewport={'width':1280,'height':900})
        nojs.route('**/*',serve)
        np=nojs.new_page();visit(np,regex_path+'?audit=1')
        assert np.locator('[data-contract]').is_visible()
        assert not np.locator('.audit-panel').is_visible()
        assert np.locator('code[data-literal="/contract/value"]').text_content()==expected
        outcomes.append({'check':'no-javascript-contract-fallback','result':'pass'})
        assert not page_errors,page_errors
        version=browser.version
        browser.close()
    report={'scope':('non-authoritative reference; actual Chromium DOM with inlined identical assets; URL/history NOT tested' if args.dom_only else 'non-authoritative reference; actual Chromium, intercepted static URLs, not live Pages or maintainer review'),'browser':version,'dom_only':args.dom_only,'results':outcomes,'javascript_errors':page_errors}
    text=json.dumps(report,indent=2)+'\n'
    if args.report:args.report.write_text(text)
    print(text)


if __name__=='__main__':main()
