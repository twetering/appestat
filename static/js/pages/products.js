import { api, h, mount, fmt, filters, filterBar, dataTable, categoryButton, setProductCategory, categoryTree, errorBox, skeleton, storeBadge } from '../core.js';

const params = new URLSearchParams(location.search);
const state = { q: params.get('q') || '', category: params.get('category') || '', subcategory: params.get('subcategory') || '' };
let table;

function updateUrl() {
  const q = new URLSearchParams(location.search);
  for (const k of ['q', 'category', 'subcategory']) state[k] ? q.set(k, state[k]) : q.delete(k);
  history.replaceState(null, '', `${location.pathname}?${q}`);
}

async function renderSearchBar() {
  const { tree } = await categoryTree();
  const search = h('input', { type: 'search', class: 'search', placeholder: 'Zoek product, merk of bonnaam… (bv. red bull)', value: state.q, 'aria-label': 'Zoek product' });
  let timer;
  search.addEventListener('input', () => { clearTimeout(timer); timer = setTimeout(() => { state.q = search.value.trim(); updateUrl(); loadList(); }, 250); });
  const cat = h('select', { 'aria-label': 'Categorie' }, h('option', { value: '' }, 'Alle categorieën'),
    Object.keys(tree).map(c => h('option', { value: c, selected: c === state.category ? true : null }, c)));
  const sub = h('select', { 'aria-label': 'Subcategorie' });
  const fillSubs = () => mount(sub, h('option', { value: '' }, 'Alle subcategorieën'),
    (tree[cat.value] || []).map(s => h('option', { value: s, selected: s === state.subcategory ? true : null }, s)));
  fillSubs();
  cat.addEventListener('change', () => { state.category = cat.value; state.subcategory = ''; fillSubs(); updateUrl(); loadList(); });
  sub.addEventListener('change', () => { state.subcategory = sub.value; updateUrl(); loadList(); });
  mount('#search-bar', search, cat, sub);
}

function sourceBadge(p) {
  if (p.suggested_category) return h('span', { class: 'badge brand', title: `AI stelt voor: ${p.suggested_category} › ${p.suggested_subcategory || ''}` }, 'suggestie');
  if (p.category_source === 'user') return null;
  if (p.needs_review) return h('span', { class: 'badge warn', title: `AI-zekerheid ${fmt.pct((p.category_confidence || 0) * 100, 0)}` }, 'controleren');
  return null;
}

async function loadList() {
  const f = filters.get();
  const q = new URLSearchParams({ sort: 'spent', limit: '2000' });
  if (state.q) q.set('q', state.q);
  if (state.category) q.set('category', state.category);
  if (state.subcategory) q.set('subcategory', state.subcategory);
  if (f.year) q.set('year', f.year);
  if (f.chain) q.set('chain', f.chain);
  if (!table) mount('#list', skeleton(8));
  try {
    const rows = await api(`/api/catalog?${q}`);
    const spent = rows.reduce((a, r) => a + r.spent, 0);
    mount('#summary',
      h('div', { class: 'kpi' }, h('div', { class: 'label' }, 'Producten'), h('div', { class: 'value' }, fmt.num(rows.length))),
      h('div', { class: 'kpi' }, h('div', { class: 'label' }, 'Uitgegeven'), h('div', { class: 'value' }, fmt.eur(spent))),
      h('div', { class: 'kpi' }, h('div', { class: 'label' }, 'Aankopen'), h('div', { class: 'value' }, fmt.num(rows.reduce((a, r) => a + r.purchases, 0)))));
    const columns = [
      { key: 'name', label: 'Product', className: 'name', render: p => h('span', {},
        h('a', { href: `/products/${p.id}` }, p.name), ' ', sourceBadge(p),
        h('span', { class: 'sub' }, [p.family, p.variants > 1 ? `${p.variants} varianten` : null].filter(Boolean).join(' · '))) },
      { key: 'category', label: 'Categorie', render: p => categoryButton({
        category: p.category, subcategory: p.subcategory,
        onPick: async (c, s) => { await setProductCategory(p.id, c, s); p.category = c; p.subcategory = s; p.category_source = 'user'; p.needs_review = 0; p.suggested_category = null; table.update(rows); },
      }) },
      { key: 'chains', label: 'Winkel', className: 'hide-mobile', render: p => h('span', { class: 'row' }, (p.chains || '').split(',').filter(Boolean).map(storeBadge)) },
      { key: 'purchases', label: 'Gekocht', num: true, className: 'hide-mobile' },
      { key: 'unit_price_avg', label: 'Per stuk', num: true, className: 'hide-mobile', render: p => fmt.unit(p.unit_price_avg) },
      { key: 'spent', label: 'Uitgegeven', num: true, render: p => fmt.eur(p.spent) },
      { key: 'last_bought', label: 'Laatst', className: 'hide-mobile nowrap', render: p => fmt.date(p.last_bought) },
    ];
    if (table) table.update(rows);
    else mount('#list', table = dataTable({ columns, rows, sortKey: 'spent', pageSize: 50, className: 'stack-mobile',
      onRow: p => { location.href = `/products/${p.id}`; }, emptyText: 'Geen producten gevonden' }));
  } catch (err) {
    mount('#list', errorBox(err));
  }
}

async function init() {
  const o = await api('/api/meta').catch(() => ({ available_years: [], chains: [] }));
  const draw = () => mount('#filters', filterBar({ years: o.available_years, chains: o.chains, onChange: () => { draw(); loadList(); } }));
  draw();
  await renderSearchBar();
  loadList();
}

init();
