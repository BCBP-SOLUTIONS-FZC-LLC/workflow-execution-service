package http

import (
	"bytes"
	"encoding/json"
	"fmt"
	"html"
	"net/http"
	"os"
	"sort"
	"strings"

	"github.com/gin-gonic/gin"
	"gopkg.in/yaml.v3"
)

const asyncAPISpecPath = "api/asyncapi.yaml"

type asyncSpec struct {
	AsyncAPI string          `yaml:"asyncapi"`
	Info     asyncInfo       `yaml:"info"`
	Servers  yaml.Node       `yaml:"servers"`
	Channels yaml.Node       `yaml:"channels"`
	Ops      yaml.Node       `yaml:"operations"`
	Comps    asyncComponents `yaml:"components"`
}

type asyncInfo struct {
	Title   string `yaml:"title"`
	Version string `yaml:"version"`
	Desc    string `yaml:"description"`
}

type asyncComponents struct {
	Messages map[string]asyncMessage `yaml:"messages"`
	Schemas  map[string]asyncSchema  `yaml:"schemas"`
}

type asyncMessage struct {
	Name        string    `yaml:"name"`
	Title       string    `yaml:"title"`
	Summary     string    `yaml:"summary"`
	Desc        string    `yaml:"description"`
	ContentType string    `yaml:"contentType"`
	Payload     asyncRef  `yaml:"payload"`
	Bindings    yaml.Node `yaml:"bindings"`
}

type asyncRef struct {
	Ref string `yaml:"$ref"`
}

type asyncSchema struct {
	Type          string               `yaml:"type"`
	Required      []string             `yaml:"required"`
	Properties    map[string]asyncProp `yaml:"properties"`
	PropertyOrder []string             // YAML insertion order; populated by UnmarshalYAML
	AllOf         []asyncSchema        `yaml:"allOf"`
	Ref           string               `yaml:"$ref"`
}

func (s *asyncSchema) UnmarshalYAML(value *yaml.Node) error {
	type rawSchema asyncSchema // breaks the method set so Decode doesn't recurse here
	if err := value.Decode((*rawSchema)(s)); err != nil {
		return fmt.Errorf("decode asyncapi schema node: %w", err)
	}
	for i := 0; i+1 < len(value.Content); i += 2 {
		if value.Content[i].Value == "properties" {
			propsNode := value.Content[i+1]
			if propsNode.Kind == yaml.MappingNode {
				s.PropertyOrder = make([]string, 0, len(propsNode.Content)/2)
				for j := 0; j+1 < len(propsNode.Content); j += 2 {
					s.PropertyOrder = append(s.PropertyOrder, propsNode.Content[j].Value)
				}
			}
			break
		}
	}
	return nil
}

type asyncProp struct {
	Type    string     `yaml:"type"`
	Format  string     `yaml:"format"`
	Desc    string     `yaml:"description"`
	Example any        `yaml:"example"`
	Enum    []string   `yaml:"enum"`
	Ref     string     `yaml:"$ref"`
	Items   *asyncProp `yaml:"items"`
}

// UnmarshalYAML lets Type accept either a scalar ("string") or the
// JSON-Schema nullable-field idiom (["string", "null"]), joining a sequence
// with "|" and dropping "null" — plain yaml.Unmarshal into a string field
// errors on the sequence form, which this spec's own promoted_from_version_id
// property already uses.
func (p *asyncProp) UnmarshalYAML(value *yaml.Node) error {
	type rawProp struct {
		Type    yaml.Node  `yaml:"type"`
		Format  string     `yaml:"format"`
		Desc    string     `yaml:"description"`
		Example any        `yaml:"example"`
		Enum    []string   `yaml:"enum"`
		Ref     string     `yaml:"$ref"`
		Items   *asyncProp `yaml:"items"`
	}
	var raw rawProp
	if err := value.Decode(&raw); err != nil {
		return fmt.Errorf("decode asyncapi prop node: %w", err)
	}
	*p = asyncProp{
		Type:    flattenTypeNode(&raw.Type),
		Format:  raw.Format,
		Desc:    raw.Desc,
		Example: raw.Example,
		Enum:    raw.Enum,
		Ref:     raw.Ref,
		Items:   raw.Items,
	}
	return nil
}

func flattenTypeNode(n *yaml.Node) string {
	switch n.Kind {
	case yaml.SequenceNode:
		var parts []string
		for _, c := range n.Content {
			if c.Value != "null" {
				parts = append(parts, c.Value)
			}
		}
		return strings.Join(parts, "|")
	case yaml.ScalarNode:
		return n.Value
	case yaml.DocumentNode, yaml.MappingNode, yaml.AliasNode:
		// "type:" is never one of these shapes in an AsyncAPI/JSON-Schema
		// property — fall back to empty rather than guess at n.Value's
		// meaning for a node kind that doesn't carry a scalar value.
		return ""
	default:
		return ""
	}
}

func AsyncAPIHandler(c *gin.Context) {
	raw, err := os.ReadFile(asyncAPISpecPath)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "could not read AsyncAPI spec: " + err.Error()})
		return
	}

	var spec asyncSpec
	if err := yaml.Unmarshal(raw, &spec); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "could not parse AsyncAPI spec: " + err.Error()})
		return
	}

	var buf bytes.Buffer
	renderPage(&buf, &spec, os.Getenv("ENV"))

	c.Header("Content-Type", "text/html; charset=utf-8")
	c.String(http.StatusOK, buf.String())
}

func renderPage(w *bytes.Buffer, s *asyncSpec, env string) {
	w.WriteString("<!DOCTYPE html>\n<html lang=\"en\" data-theme=\"dark\">\n<head>\n<meta charset=\"UTF-8\">\n<meta name=\"viewport\" content=\"width=device-width,initial-scale=1\">\n<title>")
	w.WriteString(html.EscapeString(s.Info.Title))
	w.WriteString(` — AsyncAPI</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link href="https://fonts.googleapis.com/css2?family=Poppins:wght@300;400;500;600;700&display=swap" rel="stylesheet">
<style>
:root{--bg:#000;--surface:#0d0d0d;--border:rgba(132,38,176,.25);--accent:#bd0283;--green:#22c55e;--yellow:#f59e0b;--red:#ec4b3c;--text:#fff;--muted:#9ca3af;--code:#e879f9}
html{scroll-behavior:smooth}
*{box-sizing:border-box;margin:0;padding:0}
body{font-family:"Poppins",sans-serif;background:var(--bg);color:var(--text);display:flex;flex-direction:column;min-height:100vh;font-size:14px}
a{color:var(--accent);text-decoration:none}
/* Header */
.header{background:#000;border-bottom:2px solid;border-image:linear-gradient(to right,#8426b0 3%,#bd0283 47%,#ec4b3c 98%) 1;padding:1rem 1.5rem;display:flex;align-items:center;gap:1rem;position:sticky;top:0;z-index:10}
.header h1{font-size:1.15rem;font-weight:700;background:linear-gradient(to right,#8426b0 3%,#bd0283 47%,#ec4b3c 98%);-webkit-background-clip:text;-webkit-text-fill-color:transparent;background-clip:text}
.header p{color:var(--muted);font-size:.82rem;margin-top:2px}
.badge{padding:2px 8px;border-radius:4px;font-size:.7rem;font-weight:700;white-space:nowrap}
.badge-version{background:linear-gradient(to right,#8426b0 3%,#bd0283 47%,#ec4b3c 98%);color:#fff}
/* Layout */
.layout{display:flex;flex:1}
/* Sidebar */
.sidebar{width:220px;min-width:220px;background:#050505;border-right:1px solid var(--border);padding:1rem 0;position:sticky;top:57px;height:calc(100vh - 57px);overflow-y:auto}
.sidebar-group{margin-bottom:.5rem}
.sidebar-label{padding:.35rem 1rem;font-size:.68rem;font-weight:700;text-transform:uppercase;letter-spacing:.08em;color:var(--muted)}
.sidebar a{display:block;padding:.35rem 1rem .35rem 1.25rem;color:#d1d5db;font-size:.82rem;border-left:2px solid transparent;transition:all .15s}
.sidebar a:hover{background:linear-gradient(to right,#8426b0 3%,#bd0283 47%,#ec4b3c 98%);-webkit-background-clip:text;-webkit-text-fill-color:transparent;background-clip:text;border-left-color:#8426b0}
/* Content */
.content{flex:1;padding:2rem;max-width:900px;overflow-x:auto}
.section{margin-bottom:2.5rem;scroll-margin-top:72px}
.section-title{font-size:1rem;font-weight:700;background:linear-gradient(to right,#8426b0 3%,#bd0283 47%,#ec4b3c 98%);-webkit-background-clip:text;-webkit-text-fill-color:transparent;background-clip:text;margin-bottom:1rem;padding-bottom:.5rem;border-bottom:1px solid var(--border)}
/* Cards */
.card{background:var(--surface);border:1px solid rgba(132,38,176,.3);border-radius:8px;overflow:hidden;margin-bottom:1rem;position:relative;scroll-margin-top:72px}
.card::before{content:'';position:absolute;top:0;left:0;right:0;height:2px;background:linear-gradient(to right,#8426b0 3%,#bd0283 47%,#ec4b3c 98%)}
.card-header{padding:.75rem 1rem;border-bottom:1px solid var(--border);display:flex;align-items:center;gap:.6rem;flex-wrap:wrap;cursor:pointer;user-select:none}
.card-header:hover{background:rgba(132,38,176,.06)}
.card-title{font-weight:600;color:#fff;font-size:.9rem}
.card-summary{color:var(--muted);font-size:.82rem;margin-top:2px}
.card-body{padding:1rem}
/* Method badges */
.m-send{background:linear-gradient(to right,#8426b0,#bd0283);color:#fff}
.m-recv{background:linear-gradient(to right,#bd0283,#ec4b3c);color:#fff}
/* Info box */
.info-desc{background:var(--surface);border:1px solid var(--border);border-radius:8px;padding:1.25rem;white-space:pre-wrap;font-size:.82rem;line-height:1.7;color:#d1d5db;max-height:280px;overflow-y:auto}
/* Property table */
.prop-table{width:100%;border-collapse:collapse;font-size:.8rem}
.prop-table th{background:#0a0a0a;padding:.5rem .75rem;text-align:left;color:var(--muted);font-weight:600;border-bottom:1px solid var(--border)}
.prop-table td{padding:.5rem .75rem;border-bottom:1px solid rgba(132,38,176,.12);vertical-align:top}
.prop-table tr:last-child td{border-bottom:none}
.prop-table tr:has(td:nth-child(3):not(:empty)){background:rgba(236,75,60,.07)}
.prop-table tr td:nth-child(3):not(:empty){background:rgba(236,75,60,.12)}
.prop-name{font-family:monospace;color:var(--code);font-size:.8rem}
.prop-type{color:var(--yellow);font-size:.75rem;font-family:monospace}
.prop-req{color:var(--red);font-size:.7rem;font-weight:700}
.prop-desc{color:var(--muted);font-size:.78rem;max-width:400px}
.enum-list{margin-top:4px;display:flex;flex-wrap:wrap;gap:4px}
.enum-val{background:#0d0d0d;border:1px solid rgba(132,38,176,.4);border-radius:4px;padding:1px 6px;font-family:monospace;font-size:.72rem;color:#e879f9}
/* Server list */
.server-item{padding:.6rem .75rem;border-bottom:1px solid rgba(132,38,176,.12);display:grid;grid-template-columns:120px 1fr;gap:.5rem}
.server-item:last-child{border-bottom:none}
.server-name{font-family:monospace;color:var(--code);font-size:.8rem}
.server-host{font-family:monospace;color:#d1d5db;font-size:.78rem}
.server-proto{padding:1px 6px;border-radius:4px;font-size:.68rem;font-weight:700;background:linear-gradient(to right,#8426b0,#bd0283);color:#fff;display:inline-block}
.server-desc{color:var(--muted);font-size:.76rem;grid-column:1/-1;white-space:pre-wrap}
.sns-attr{padding:.2rem .5rem;border-radius:4px;background:#0d0d0d;border:1px solid rgba(132,38,176,.4);font-family:monospace;font-size:.75rem;color:#e879f9;display:inline-block;margin:2px}
@keyframes field-flash{0%,100%{background:transparent}30%{background:rgba(189,2,131,.35)}}
.field-highlight{animation:field-flash 1.6s ease}
.toggle-btn{margin-left:auto;background:none;border:1px solid rgba(132,38,176,.5);color:#e879f9;border-radius:4px;padding:2px 8px;font-size:.75rem;cursor:pointer;transition:border-color .15s,color .15s;flex-shrink:0;pointer-events:none}
.toggle-btn:hover{border-color:#8426b0;color:#fff}
/* Theme toggle button */
.theme-toggle{background:none;border:1px solid rgba(132,38,176,.5);color:#9ca3af;border-radius:20px;padding:3px 10px;font-size:.78rem;cursor:pointer;transition:border-color .15s,color .15s;white-space:nowrap;font-family:"Poppins",sans-serif}
.theme-toggle:hover{border-color:#bd0283;color:#fff}
/* Light mode */
html[data-theme="light"]{--bg:#f5f5f7;--surface:#fff;--border:rgba(132,38,176,.22);--muted:#6b7280;--code:#8426b0;--text:#111}
html[data-theme="light"] body{background:#f5f5f7;color:#111}
html[data-theme="light"] .header{background:#fff}
html[data-theme="light"] .sidebar{background:#ebebed}
html[data-theme="light"] .sidebar a{color:#374151}
html[data-theme="light"] .card{background:#fff}
html[data-theme="light"] .card-header:hover{background:rgba(132,38,176,.05)}
html[data-theme="light"] .card-title{color:#111}
html[data-theme="light"] .prop-table th{background:#f3f4f6}
html[data-theme="light"] .prop-table td{border-bottom-color:rgba(132,38,176,.08)}
html[data-theme="light"] .info-desc{background:#fff;color:#374151}
html[data-theme="light"] .enum-val{background:#fff;color:#8426b0}
html[data-theme="light"] .sns-attr{background:#fff;color:#8426b0}
html[data-theme="light"] .server-host{color:#374151}
html[data-theme="light"] #search{background:#fff!important;color:#111!important;border-color:rgba(132,38,176,.3)!important}
mark.search-mark{background:rgba(189,2,131,.3);color:inherit;border-radius:2px;padding:0 1px}
html[data-theme="light"] mark.search-mark{background:rgba(132,38,176,.2)}
</style>
</head>
<body>
`)

	envLabel := strings.ToUpper(env)
	if envLabel == "" {
		envLabel = "DEV"
	}
	envColor := "#22c55e" // green — dev/unknown
	switch strings.ToLower(env) {
	case "prod", "production":
		envColor = "#ec4b3c"
	case "staging", "stage":
		envColor = "#f59e0b"
	}
	fmt.Fprintf(w, `<div class="header">
  <div>
    <h1>%s</h1>
    <p>Events published to <strong>wf-workflow-events</strong> SNS topic</p>
  </div>
  <span class="badge badge-version">v%s</span>
  <span class="badge" style="background:linear-gradient(to right,#bd0283,#ec4b3c);color:#fff">AsyncAPI %s</span>
  <span class="badge" style="background:%s;color:#fff;margin-left:auto">%s</span>
  <button id="theme-btn" class="theme-toggle" onclick="toggleTheme()" title="Toggle light/dark mode">🌙</button>
</div>
`, html.EscapeString(s.Info.Title), html.EscapeString(s.Info.Version), html.EscapeString(s.AsyncAPI),
		envColor, html.EscapeString(envLabel))

	w.WriteString(`<div class="layout"><nav class="sidebar">`)
	w.WriteString(`<div style="padding:.75rem 1rem .25rem"><input id="search" placeholder="Search..." autocomplete="off" style="width:100%;padding:6px 10px;background:#0d0d0d;border:1px solid var(--border);color:#fff;border-radius:6px;font-size:.8rem;outline:none"></div>`)
	w.WriteString(`<div class="sidebar-group"><div class="sidebar-label">Overview</div>`)
	w.WriteString(`<a href="#info">Info</a><a href="#servers">Servers</a></div>`)
	w.WriteString(`<div class="sidebar-group"><div class="sidebar-label">Messages</div>`)
	for _, name := range sortedKeys(s.Comps.Messages) {
		fmt.Fprintf(w, `<a href="#msg-%s">%s</a>`, name, html.EscapeString(name))
	}
	w.WriteString(`</div>`)
	w.WriteString(`<div class="sidebar-group"><div class="sidebar-label">Schemas</div>`)
	for _, name := range sortedKeys(s.Comps.Schemas) {
		fmt.Fprintf(w, `<a href="#schema-%s">%s</a>`, name, html.EscapeString(name))
	}
	w.WriteString(`</div></nav>`)

	w.WriteString(`<main class="content">`)

	w.WriteString(`<div class="section" id="info">`)
	w.WriteString(`<div class="section-title">Info</div>`)
	if s.Info.Desc != "" {
		short := s.Info.Desc
		if len(short) > 800 {
			short = short[:800] + "\n\n[truncated — see api/asyncapi.yaml for full description]"
		}
		fmt.Fprintf(w, `<div class="info-desc">%s</div>`, html.EscapeString(short))
	}
	w.WriteString(`</div>`)

	renderServers(w, &s.Servers)

	w.WriteString(`<div class="section" id="messages"><div class="section-title">Messages</div>`)
	for _, name := range sortedKeys(s.Comps.Messages) {
		msg, ok := s.Comps.Messages[name]
		if !ok {
			continue
		}
		renderMessage(w, name, &msg, &s.Comps)
	}
	w.WriteString(`</div>`)

	w.WriteString(`<div class="section" id="schemas"><div class="section-title">Schemas</div>`)
	for _, name := range sortedKeys(s.Comps.Schemas) {
		sc, ok := s.Comps.Schemas[name]
		if !ok {
			continue
		}
		renderSchema(w, name, &sc, &s.Comps)
	}
	w.WriteString(`</div>`)

	w.WriteString(`</main></div>
<button onclick="window.scrollTo({top:0,behavior:'smooth'})" title="Back to top" style="position:fixed;bottom:24px;right:24px;width:40px;height:40px;background:linear-gradient(135deg,#8426b0,#bd0283);color:#fff;border:none;border-radius:50%;font-size:1.1rem;cursor:pointer;box-shadow:0 4px 12px rgba(132,38,176,.4);z-index:100;display:flex;align-items:center;justify-content:center;transition:transform .15s" onmouseover="this.style.transform='scale(1.1)'" onmouseout="this.style.transform='scale(1)'">↑</button>
<script>
document.addEventListener('keydown',function(e){
  var search=document.getElementById('search');
  if(e.key==='/'&&document.activeElement!==search){e.preventDefault();search.focus();search.select();}
  if(e.key==='Escape'&&document.activeElement===search){search.blur();}
});
var _HL='.prop-name,.card-title,.card-summary,.prop-desc,.server-name,.server-host,.server-desc,.badge-version';
(function(){document.querySelectorAll(_HL).forEach(function(el){el.dataset.orig=el.textContent;});})();
function _highlight(term){
  document.querySelectorAll(_HL).forEach(function(el){
    var orig=el.dataset.orig!=null?el.dataset.orig:el.textContent;
    if(!term){el.textContent=orig;return;}
    var lower=orig.toLowerCase();
    if(lower.indexOf(term)===-1){el.textContent=orig;return;}
    var frag=document.createDocumentFragment(),rem=orig,lrem=lower;
    while(lrem.indexOf(term)!==-1){
      var i=lrem.indexOf(term);
      if(i>0)frag.appendChild(document.createTextNode(rem.slice(0,i)));
      var m=document.createElement('mark');m.className='search-mark';
      m.textContent=rem.slice(i,i+term.length);
      frag.appendChild(m);
      rem=rem.slice(i+term.length);lrem=rem.toLowerCase();
    }
    if(rem)frag.appendChild(document.createTextNode(rem));
    el.textContent='';el.appendChild(frag);
  });
}
var _searchT;
document.getElementById('search').addEventListener('input',function(e){
  clearTimeout(_searchT);
  var val=e.target.value.toLowerCase();
  _searchT=setTimeout(function(){
    document.querySelectorAll('.card').forEach(function(card){
      card.style.display=card.innerText.toLowerCase().includes(val)||!val?'':'none';
    });
    _highlight(val);
  },120);
});
function copyText(text){
  navigator.clipboard.writeText(text).then(function(){
    var t=document.createElement('div');
    t.textContent='Copied!';
    t.style.cssText='position:fixed;bottom:72px;right:24px;background:linear-gradient(to right,#8426b0,#bd0283);color:#fff;padding:6px 14px;border-radius:6px;font-size:.78rem;font-weight:600;font-family:"Poppins",sans-serif;box-shadow:0 4px 12px rgba(132,38,176,.4);pointer-events:none;z-index:999;opacity:1;transition:opacity .3s';
    document.body.appendChild(t);
    setTimeout(function(){t.style.opacity='0';},700);
    setTimeout(function(){t.remove();},1000);
  });
}
function setCardExpanded(card,expanded){
  var header=card.querySelector('.card-header');
  var body=card.querySelector('.card-body');
  var btn=card.querySelector('.toggle-btn');
  if(!body)return;
  body.style.display=expanded?'':'none';
  if(btn)btn.textContent=expanded?'▾':'▸';
  if(header)header.setAttribute('aria-expanded',expanded?'true':'false');
}
function toggleCard(header){
  var body=header.closest('.card').querySelector('.card-body');
  setCardExpanded(header.closest('.card'),body.style.display==='none');
}
function expandSchemaCard(schemaId){
  var card=document.getElementById(schemaId);
  if(card)setCardExpanded(card,true);
}
document.querySelectorAll('.card-header').forEach(function(header){
  header.addEventListener('click',function(){toggleCard(header);});
  header.addEventListener('keydown',function(e){
    if(e.key==='Enter'||e.key===' '){e.preventDefault();toggleCard(header);}
  });
  header.setAttribute('role','button');
  header.setAttribute('tabindex','0');
  header.setAttribute('aria-expanded','false');
});
document.querySelectorAll('a[href^="#schema-"]').forEach(function(a){
  a.addEventListener('click',function(){
    var id=a.getAttribute('href').slice(1);
    setTimeout(function(){expandSchemaCard(id);},0);
  });
});
function deepLinkField(hash){
  var m=hash.match(/^#schema-([^.]+)\.(.+)$/);
  if(!m)return;
  var rowId='field--'+m[1]+'--'+m[2];
  var row=document.getElementById(rowId);
  if(!row)return;
  var card=row.closest('.card');
  if(card)setCardExpanded(card,true);
  setTimeout(function(){
    row.scrollIntoView({behavior:'smooth',block:'center'});
    row.classList.remove('field-highlight');
    void row.offsetWidth;
    row.classList.add('field-highlight');
  },120);
}
window.addEventListener('hashchange',function(){
  var h=window.location.hash;
  if(h.indexOf('#schema-')===0){
    if(h.indexOf('.')!==-1){deepLinkField(h);}
    else{expandSchemaCard(h.slice(1));}
  }
});
if(window.location.hash){deepLinkField(window.location.hash);}
function toggleTheme(){
  var html=document.documentElement;
  var next=html.getAttribute('data-theme')==='light'?'dark':'light';
  html.setAttribute('data-theme',next);
  var btn=document.getElementById('theme-btn');
  if(btn)btn.textContent=next==='light'?'☀️':'🌙';
  try{localStorage.setItem('asyncapi-theme',next);}catch(e){}
}
(function(){
  var saved;try{saved=localStorage.getItem('asyncapi-theme');}catch(e){}
  if(saved==='light'){
    document.documentElement.setAttribute('data-theme','light');
    var btn=document.getElementById('theme-btn');
    if(btn)btn.textContent='☀️';
  }
})();
(function(){
  var links=document.querySelectorAll('.sidebar a[href^="#"]');
  function activate(){
    var fromTop=window.scrollY+80;
    var active=null;
    links.forEach(function(link){
      var section=document.getElementById(link.getAttribute('href').slice(1));
      if(section&&section.offsetTop<=fromTop&&section.offsetTop+section.offsetHeight>fromTop){
        active=link;
      }
      link.style.cssText='';
    });
    if(active){
      active.style.background='linear-gradient(to right,#8426b0 3%,#bd0283 47%,#ec4b3c 98%)';
      active.style.webkitBackgroundClip='text';
      active.style.webkitTextFillColor='transparent';
      active.style.backgroundClip='text';
      active.style.borderLeftColor='#8426b0';
    }
  }
  window.addEventListener('scroll',activate,{passive:true});
  activate();
})();
window.addEventListener('beforeunload',function(){
  try{localStorage.setItem('asyncapi-scrollY',window.scrollY);}catch(e){}
});
(function(){
  var y;try{y=localStorage.getItem('asyncapi-scrollY');}catch(e){}
  if(y&&!window.location.hash){window.scrollTo(0,parseInt(y,10));}
})();
</script>
</body></html>`)
}

func renderServers(w *bytes.Buffer, node *yaml.Node) {
	if node == nil || node.Kind == 0 {
		return
	}
	w.WriteString(`<div class="section" id="servers"><div class="section-title">Servers</div>`)
	w.WriteString(`<div class="card">`)

	if node.Kind == yaml.MappingNode {
		for i := 0; i+1 < len(node.Content); i += 2 {
			name := node.Content[i].Value
			val := node.Content[i+1]

			host, proto, desc := "", "", ""
			for j := 0; j+1 < len(val.Content); j += 2 {
				switch val.Content[j].Value {
				case "host":
					host = val.Content[j+1].Value
				case "protocol":
					proto = val.Content[j+1].Value
				case "description":
					desc = val.Content[j+1].Value
					if len(desc) > 200 {
						desc = desc[:200] + "…"
					}
				}
			}
			fmt.Fprintf(w, `<div class="server-item">
  <div><span class="server-name">%s</span></div>
  <div><span class="server-proto">%s</span> <span class="server-host">%s</span></div>
  <div class="server-desc">%s</div>
</div>`, html.EscapeString(name), html.EscapeString(strings.ToUpper(proto)),
				html.EscapeString(host), html.EscapeString(strings.TrimSpace(desc)))
		}
	}
	w.WriteString(`</div></div>`)
}

// isInboundMessage reports whether a components.messages key documents a
// consumed (receive) event rather than one this service publishes. Inbound
// entries deliberately carry an "Inbound" key suffix — see api/asyncapi.yaml
// — since their payload schema is owned/registered by the producing service,
// not this one.
func isInboundMessage(name string) bool {
	return strings.HasSuffix(name, "Inbound")
}

func renderMessage(w *bytes.Buffer, name string, msg *asyncMessage, comps *asyncComponents) {
	title := msg.Title
	if title == "" {
		title = name
	}
	summary := strings.TrimSpace(msg.Summary)
	if summary == "" {
		summary = strings.TrimSpace(msg.Desc)
	}

	eventType := snsEventType(&msg.Bindings)

	badgeClass, badgeLabel := "m-send", "SEND"
	if isInboundMessage(name) {
		badgeClass, badgeLabel = "m-recv", "RECEIVE"
	}

	fmt.Fprintf(w, `<div class="card" id="msg-%s">
<div class="card-header">
  <span class="badge %s">%s</span>
  <span class="card-title">%s</span>
  %s
  <button class="toggle-btn" type="button" tabindex="-1" aria-hidden="true">▸</button>
</div>
<div class="card-body" style="display:none">
`, name, badgeClass, badgeLabel, html.EscapeString(title),
		func() string {
			if eventType != "" {
				return fmt.Sprintf(`<span class="sns-attr">event_type: %s</span>`, html.EscapeString(eventType))
			}
			return ""
		}())

	if summary != "" {
		fmt.Fprintf(w, `<p class="card-summary" style="margin-bottom:.75rem">%s</p>`, html.EscapeString(summary))
	}

	payloadRef := strings.TrimPrefix(msg.Payload.Ref, "#/components/schemas/")
	if payloadRef != "" {
		if sc, ok := comps.Schemas[payloadRef]; ok {
			fmt.Fprintf(w, `<div style="font-size:.78rem;color:var(--muted);margin-bottom:.5rem">Payload: <span style="color:var(--code);font-family:monospace">%s</span></div>`, html.EscapeString(payloadRef))
			renderPropsTable(w, resolveSchema(&sc, comps), payloadRef)
		}
	}

	w.WriteString(`</div></div>`)
}

func renderSchema(w *bytes.Buffer, name string, sc *asyncSchema, comps *asyncComponents) {
	fmt.Fprintf(w, `<div class="card" id="schema-%s">
<div class="card-header"><span class="card-title" style="font-family:monospace">%s</span><button class="toggle-btn" type="button" tabindex="-1" aria-hidden="true">▸</button></div>
<div class="card-body" style="display:none">`, name, html.EscapeString(name))
	renderPropsTable(w, resolveSchema(sc, comps), name)
	w.WriteString(`</div></div>`)
}

func renderPropsTable(w *bytes.Buffer, sc *asyncSchema, schemaID string) {
	if sc == nil || len(sc.Properties) == 0 {
		w.WriteString(`<p style="color:var(--muted);font-size:.8rem">No properties.</p>`)
		return
	}
	reqSet := make(map[string]bool, len(sc.Required))
	for _, r := range sc.Required {
		reqSet[r] = true
	}

	w.WriteString(`<table class="prop-table"><thead><tr>
<th>Field</th><th>Type</th><th>Required</th><th>Description</th>
</tr></thead><tbody>`)

	keys := sc.PropertyOrder
	if len(keys) == 0 {
		keys = sortedKeys(sc.Properties)
	}
	seen := make(map[string]bool, len(keys))
	for _, k := range keys {
		seen[k] = true
	}
	for _, k := range sortedKeys(sc.Properties) {
		if !seen[k] {
			keys = append(keys, k)
		}
	}
	sort.SliceStable(keys, func(i, j int) bool {
		return reqSet[keys[i]] && !reqSet[keys[j]]
	})
	for _, k := range keys {
		v := sc.Properties[k]
		typeStr := typeHTML(&v)
		req := ""
		if reqSet[k] {
			req = `<span class="prop-req">required</span>`
		}
		desc := strings.TrimSpace(v.Desc)
		if len(desc) > 200 {
			desc = desc[:200] + "…"
		}
		descHTML := html.EscapeString(desc)

		enumHTML := ""
		if len(v.Enum) > 0 {
			var eb strings.Builder
			eb.WriteString(`<div class="enum-list">`)
			for _, ev := range v.Enum {
				eb.WriteString(`<span class="enum-val">`)
				eb.WriteString(html.EscapeString(ev))
				eb.WriteString(`</span>`)
			}
			eb.WriteString(`</div>`)
			enumHTML = eb.String()
		}
		itemEnumHTML := ""
		if v.Items != nil && len(v.Items.Enum) > 0 {
			var eb strings.Builder
			eb.WriteString(`<div style="margin-top:4px;font-size:.72rem;color:var(--muted)">values:</div><div class="enum-list">`)
			for _, ev := range v.Items.Enum {
				eb.WriteString(`<span class="enum-val">`)
				eb.WriteString(html.EscapeString(ev))
				eb.WriteString(`</span>`)
			}
			eb.WriteString(`</div>`)
			itemEnumHTML = eb.String()
		}

		exampleHTML := ""
		if v.Example != nil {
			if b, err := json.MarshalIndent(v.Example, "", "  "); err == nil {
				exampleHTML = `<pre style="background:#0d0d0d;border:1px solid rgba(132,38,176,.25);padding:6px 8px;border-radius:6px;font-size:.72rem;margin-top:6px;color:#e879f9;overflow-x:auto;white-space:pre-wrap">` +
					html.EscapeString(string(b)) + `</pre>`
			}
		}

		rowID := ""
		if schemaID != "" {
			rowID = fmt.Sprintf(` id="field--%s--%s"`, html.EscapeString(schemaID), html.EscapeString(k))
		}
		fmt.Fprintf(w, `<tr%s>
<td onclick="copyText('%s')" title="Click to copy" style="cursor:pointer"><span class="prop-name">%s</span></td>
<td>%s</td>
<td>%s</td>
<td><span class="prop-desc">%s</span>%s%s%s</td>
</tr>`, rowID, html.EscapeString(k), html.EscapeString(k), typeStr, req, descHTML, enumHTML, itemEnumHTML, exampleHTML)
	}

	w.WriteString(`</tbody></table>`)
}

func sortedKeys[T any](m map[string]T) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func resolveSchema(sc *asyncSchema, comps *asyncComponents) *asyncSchema {
	if len(sc.AllOf) == 0 {
		if sc.Ref != "" {
			name := strings.TrimPrefix(sc.Ref, "#/components/schemas/")
			if sub, ok := comps.Schemas[name]; ok {
				return resolveSchema(&sub, comps)
			}
		}
		return sc
	}
	merged := &asyncSchema{Properties: make(map[string]asyncProp)}
	seen := make(map[string]bool)
	for _, sub := range sc.AllOf {
		resolved := resolveSchema(&sub, comps)
		if resolved == nil {
			continue
		}
		merged.Required = append(merged.Required, resolved.Required...)
		for _, k := range resolved.PropertyOrder {
			if !seen[k] {
				seen[k] = true
				merged.PropertyOrder = append(merged.PropertyOrder, k)
			}
		}
		for k, v := range resolved.Properties {
			merged.Properties[k] = v
		}
	}
	return merged
}

func propType(p *asyncProp) string {
	if p.Ref != "" {
		return strings.TrimPrefix(p.Ref, "#/components/schemas/")
	}
	t := p.Type
	if p.Format != "" {
		t += "(" + p.Format + ")"
	}
	if p.Type == "array" && p.Items != nil {
		if p.Items.Type != "" {
			t = "array<" + p.Items.Type + ">"
		} else if p.Items.Ref != "" {
			t = "array<" + strings.TrimPrefix(p.Items.Ref, "#/components/schemas/") + ">"
		}
	}
	return t
}

func typeHTML(p *asyncProp) string {
	if p.Ref != "" {
		name := strings.TrimPrefix(p.Ref, "#/components/schemas/")
		return fmt.Sprintf(`<a href="#schema-%s" class="prop-type schema-link">%s</a>`,
			html.EscapeString(name), html.EscapeString(name))
	}
	if p.Type == "array" && p.Items != nil && p.Items.Ref != "" {
		name := strings.TrimPrefix(p.Items.Ref, "#/components/schemas/")
		return fmt.Sprintf(`<span class="prop-type">array&lt;<a href="#schema-%s" class="prop-type schema-link">%s</a>&gt;</span>`,
			html.EscapeString(name), html.EscapeString(name))
	}
	return `<span class="prop-type">` + html.EscapeString(propType(p)) + `</span>`
}

func snsEventType(node *yaml.Node) string {
	if node == nil || node.Kind == 0 {
		return ""
	}
	return walkYAML(node, "sns", "messageAttributes", "event_type", "value")
}

func walkYAML(node *yaml.Node, keys ...string) string {
	if node == nil || len(keys) == 0 {
		return ""
	}
	target := keys[0]
	rest := keys[1:]

	n := node
	if n.Kind == yaml.DocumentNode && len(n.Content) > 0 {
		n = n.Content[0]
	}
	if n.Kind == yaml.MappingNode {
		for i := 0; i+1 < len(n.Content); i += 2 {
			if n.Content[i].Value == target {
				if len(rest) == 0 {
					return n.Content[i+1].Value
				}
				return walkYAML(n.Content[i+1], rest...)
			}
		}
	}
	return ""
}
