import { api, h, mount, fmt, filters, filterBar, dataTable, errorBox, skeleton, empty, toast, confirmDialog, categoryTree } from '../core.js';

const TABS = [['overzicht', 'Overzicht'], ['beheren', 'Beheren'], ['regels', 'Regels']];
let active = (location.hash || '#overzicht').slice(1);
if (!TABS.some(([k]) => k === active)) active = 'overzicht';

function tabs() {
  mount('#tabs', TABS.map(([key, label]) => h('button', {
    role: 'tab', class: key === active ? 'active' : '', 'aria-selected': String(key === active),
    onclick: () => { active = key; history.replaceState(null, '', `#${key}`); tabs(); show(); },
  }, label)));
}

// ---------- Overzicht ----------
async function overview() {
  const body = h('div', {});
  const meta = await api('/api/meta');
  const draw = async () => {
    const f = filters.get();
    mount(body, h('div', { class: 'card-body' }, filterBar({ years: meta.available_years, chains: meta.chains, onChange: draw })), skeleton(6));
    const o = await api(`/api/overview?${filters.query(f)}`);
    const total = o.summary.spent || 1;
    const rows = o.categories.map(c => {
      const subs = h('div', { class: 'sub-rows hidden' },
        c.subcategories.map(s => h('a', { class: 'cat-row', style: { color: 'inherit', textDecoration: 'none', cursor: 'pointer' },
          href: `/products?category=${encodeURIComponent(c.name)}&subcategory=${encodeURIComponent(s.name)}` },
          h('div', { class: 'name' }, h('span', { class: 't' }, s.name)),
          h('div', { class: 'bar-track' }, h('div', { class: 'bar-fill', style: { width: `${s.total / c.total * 100}%`, background: c.color } })),
          h('div', { class: 'amt' }, fmt.eur(s.total), h('span', { class: 'pct' }, `${s.count}×`)))),
        c.name !== 'Bijkomende kosten' ? h('div', { style: { padding: '.3rem 2.6rem' } },
          h('a', { class: 'small', href: `/products?category=${encodeURIComponent(c.name)}` }, `Alle producten in ${c.name} →`)) : null);
      const row = h('div', { class: 'cat-row', role: 'button', tabindex: '0' },
        h('div', { class: 'name' }, h('span', {}, c.icon), h('span', { class: 't' }, c.name)),
        h('div', { class: 'bar-track' }, h('div', { class: 'bar-fill', style: { width: `${c.total / o.categories[0].total * 100}%`, background: c.color } })),
        h('div', { class: 'amt' }, fmt.eur(c.total), h('span', { class: 'pct' }, fmt.pct(c.total / total * 100))));
      const toggle = () => subs.classList.toggle('hidden');
      row.addEventListener('click', toggle);
      row.addEventListener('keydown', e => { if (e.key === 'Enter') toggle(); });
      return [row, subs];
    });
    mount(body, h('div', { class: 'card-body' }, filterBar({ years: meta.available_years, chains: meta.chains, onChange: draw }),
      h('p', { class: 'small muted' }, `Totaal ${fmt.eur(o.summary.spent)}: de categorieën tellen precies op tot wat je uitgaf. "Bijkomende kosten" zijn statiegeld, bezorging en abonnementen.`)),
      rows.length ? rows : empty('🧾', 'Geen uitgaven in deze periode'));
  };
  draw();
  return body;
}

// ---------- Beheren ----------
async function manage() {
  const [cats, subs] = await Promise.all([api('/api/categories'), api('/api/subcategories')]);
  const byCat = {};
  subs.forEach(s => (byCat[s.category_name] ||= []).push(s));
  const fixed = new Set(['Overig', 'Bijkomende kosten']);

  const addCategory = h('form', { class: 'row', onsubmit: async e => {
    e.preventDefault();
    const name = e.target.name.value.trim();
    if (!name) return;
    try { await api('/api/categories', { method: 'POST', body: { name, icon: e.target.icon.value || '📁' } }); toast(`Categorie ${name} toegevoegd`); show(); }
    catch (err) { toast(err.message, 'error'); }
  } }, h('input', { name: 'icon', type: 'text', placeholder: '📁', style: { width: '3.5rem' }, 'aria-label': 'Icoon' }),
    h('input', { name: 'name', type: 'text', placeholder: 'Nieuwe categorie', 'aria-label': 'Naam nieuwe categorie' }),
    h('button', { class: 'btn primary', type: 'submit' }, 'Toevoegen'));

  const list = cats.map(c => {
    const addSub = h('form', { class: 'row', onsubmit: async e => {
      e.preventDefault();
      const name = e.target.sub.value.trim();
      if (!name) return;
      try { await api('/api/subcategories', { method: 'POST', body: { category: c.name, name } }); toast(`Subcategorie ${name} toegevoegd`); show(); }
      catch (err) { toast(err.message, 'error'); }
    } }, h('input', { name: 'sub', type: 'text', placeholder: 'Nieuwe subcategorie', 'aria-label': `Nieuwe subcategorie in ${c.name}` }),
      h('button', { class: 'btn small', type: 'submit' }, '+'));
    return h('div', { class: 'card-body', style: { borderBottom: '1px solid var(--border)' } },
      h('div', { class: 'row' }, h('span', {}, c.icon), h('strong', {}, c.name), h('span', { class: 'muted small' }, `${c.product_count} aankopen`),
        h('span', { class: 'spacer' }),
        fixed.has(c.name) ? h('span', { class: 'badge' }, 'vast') : h('button', { class: 'btn small danger', onclick: async () => {
          if (!await confirmDialog('Categorie verwijderen', `"${c.name}" verwijderen? De producten erin gaan naar Overig en komen in de reviewlijst.`, 'Verwijderen', true)) return;
          try { await api(`/api/categories/${c.id}`, { method: 'DELETE' }); toast('Verwijderd'); show(); } catch (err) { toast(err.message, 'error'); }
        } }, 'Verwijder')),
      h('div', { class: 'chips', style: { marginTop: '.5rem' } },
        (byCat[c.name] || []).map(s => h('span', { class: 'chip', style: { cursor: 'default' } }, s.name,
          h('span', { class: 'muted small' }, ` ${s.product_count}`),
          h('button', { class: 'btn ghost small', title: `${s.name} verwijderen`, 'aria-label': `${s.name} verwijderen`, onclick: async () => {
            if (!await confirmDialog('Subcategorie verwijderen', `"${s.name}" verwijderen? Producten houden hun categorie ${c.name}.`, 'Verwijderen', true)) return;
            await api(`/api/subcategories/${s.id}`, { method: 'DELETE' }); toast('Verwijderd'); show();
          } }, '✕'))),
        fixed.has(c.name) ? null : addSub));
  });
  return h('div', {}, h('div', { class: 'card-body' }, addCategory), list);
}

// ---------- Regels ----------
async function rules() {
  const [kw, { tree }] = await Promise.all([api('/api/categorization/keywords'), categoryTree()]);
  const body = h('div', {});
  const intro = h('div', { class: 'card-body' },
    h('p', { class: 'small muted' }, 'Nieuwe producten krijgen hun categorie van de AI (OpenAI Decisions). Deze trefwoorden zijn de snelle terugval als er geen AI beschikbaar is. Jouw eigen keuzes gaan altijd voor.'));
  const cat = h('select', { name: 'category', 'aria-label': 'Categorie' }, Object.keys(tree).map(c => h('option', { value: c }, c)));
  const sub = h('select', { name: 'subcategory', 'aria-label': 'Subcategorie' });
  const fill = () => mount(sub, h('option', { value: '' }, '(geen subcategorie)'), (tree[cat.value] || []).map(s => h('option', { value: s }, s)));
  cat.addEventListener('change', fill); fill();
  const form = h('form', { class: 'card-body row', onsubmit: async e => {
    e.preventDefault();
    const keyword = e.target.keyword.value.trim();
    if (!keyword) return;
    try { await api('/api/categorization/keywords', { method: 'POST', body: { keyword, category: cat.value, subcategory: sub.value || null } }); toast('Trefwoord toegevoegd'); show(); }
    catch (err) { toast(err.message, 'error'); }
  } }, h('input', { name: 'keyword', type: 'text', placeholder: 'trefwoord, bv. "kikkererwt"', 'aria-label': 'Trefwoord' }), cat, sub,
    h('button', { class: 'btn primary', type: 'submit' }, 'Toevoegen'),
    h('button', { class: 'btn', type: 'button', title: 'Pas de regels opnieuw toe op producten die (nog) geen AI- of eigen categorie hebben', onclick: async () => {
      const r = await api('/api/migrate', { method: 'POST' }); toast(`${r.recategorized} producten opnieuw ingedeeld`);
    } }, 'Regels toepassen'));
  mount(body, intro, form, dataTable({
    columns: [
      { key: 'keyword', label: 'Trefwoord' },
      { key: 'category_name', label: 'Categorie', render: k => [k.category_name, k.subcategory_name].filter(Boolean).join(' › ') },
      { key: 'created_by', label: 'Bron', render: k => k.created_by === 'user' ? h('span', { class: 'badge brand' }, 'jij') : h('span', { class: 'badge' }, 'standaard') },
      { key: 'del', label: '', sortable: false, render: k => h('button', { class: 'btn small ghost', 'aria-label': `${k.keyword} verwijderen`, onclick: async () => {
        await api(`/api/categorization/keywords/${k.id}`, { method: 'DELETE' }); toast('Verwijderd'); show();
      } }, '✕') },
    ], rows: kw, sortKey: 'keyword', sortDir: 'asc', pageSize: 50,
  }));
  return body;
}

async function show() {
  mount('#tab-body', h('div', { class: 'card-body' }, skeleton(6)));
  try {
    mount('#tab-body', await ({ overzicht: overview, beheren: manage, regels: rules }[active])());
  } catch (err) {
    mount('#tab-body', h('div', { class: 'card-body' }, errorBox(err)));
  }
}

window.addEventListener('hashchange', () => {
  const key = location.hash.slice(1);
  if (TABS.some(([k]) => k === key) && key !== active) { active = key; tabs(); show(); }
});
tabs();
show();
