'use strict';
const root=document.documentElement;
const audit=document.getElementById('audit-mode');
const docs=document.getElementById('docs-mode');
root.dataset.js='yes';
function apply(){const url=new URL(location.href);
  const requestedAudit=url.searchParams.get('audit');
  root.dataset.audit=(requestedAudit==='1'||
    (requestedAudit===null&&root.dataset.auditDefault==='on'))&&audit&&!audit.disabled?'on':'off';
  const requestedDocs=url.searchParams.get('docs');
  root.dataset.docs=(requestedDocs==='1'||
    (requestedDocs===null&&root.dataset.docsDefault==='on'))&&docs&&!docs.disabled?'on':'off';
  if(audit)audit.checked=root.dataset.audit==='on';
  if(docs)docs.checked=root.dataset.docs==='on';}
function changed(){const url=new URL(location.href);
  for(const [key,control] of [['audit',audit],['docs',docs]]){
    if(control&&control.checked)url.searchParams.set(key,'1');
    else if((key==='audit'&&root.dataset.auditDefault==='on')||
            (key==='docs'&&root.dataset.docsDefault==='on'))url.searchParams.set(key,'0');
    else url.searchParams.delete(key);
  }
  history.pushState(null,'',url);apply();}
if(audit)audit.addEventListener('change',changed);
if(docs)docs.addEventListener('change',changed);
addEventListener('popstate',apply);
function revealFragment(){
  if(!location.hash)return;
  const target=document.getElementById(decodeURIComponent(location.hash.slice(1)));
  for(let parent=target?.parentElement;parent;parent=parent.parentElement)
    if(parent.tagName==='DETAILS')parent.open=true;
}
addEventListener('hashchange',revealFragment);
document.addEventListener('click',event=>{
  const link=event.target.closest('a[href]');if(!link)return;
  const url=new URL(link.href,location.href);
  if(url.origin!==location.origin||!url.pathname.endsWith('.html'))return;
  for(const key of ['audit','docs'])if(root.dataset[key]==='on')url.searchParams.set(key,'1');
  else url.searchParams.delete(key);
  link.href=url.href;});
apply();
revealFragment();
const sources=[...document.querySelectorAll('code[data-source-path]')];
if(sources.length)fetch(new URL('../build-manifest.json',location.href))
  .then(response=>response.ok?response.json():null)
  .then(manifest=>{
    if(!manifest||manifest.format!=='riverhog-contract-preview-build/v1'||
       !/^[0-9a-f]{40}$/.test(manifest.source_sha))return;
    for(const source of sources){
      const path=source.dataset.sourcePath.split('/').map(encodeURIComponent).join('/');
      const line=source.dataset.sourceLine;
      const link=document.createElement('a');
      link.href='https://github.com/nashspence/riverhog/blob/'+manifest.source_sha+'/'+path+
        (line?'#L'+line:'');
      link.dataset.sourceLink='exact-commit';
      source.replaceWith(link);link.append(source);
    }
  }).catch(()=>{});
