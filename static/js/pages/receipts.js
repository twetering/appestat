import { api, h, mount, fmt, filters, filterBar, dataTable, errorBox, skeleton, storeBadge } from '../core.js';

const params = new URLSearchParams(location.search);
const state = { q: params.get('q') || '', status: params.get('status') || '' };
let table;

const SOURCES = { ah_online: 'AH online', ah_kassabon: 'AH winkel', plus_receipt: 'PLUS foto', ah_api: 'AH app' };

function status(r) {
  if (r.quality !== null && r.quality < 1) return h('span', { class: 'badge warn', title: r.parse_notes || '' }, 'controleren');
  if (r.lines && r.validated === r.lines) return h('span', { class: 'badge good' }, 'gecontroleerd');
  return h('span', { class: 'badge good', title: 'Regels tellen precies op tot het betaalde bedrag' }, 'klopt');
}

function renderSearch() {
  const input = h('input', { type: 'search', class: 'search', value: state.q, placeholder: 'Zoek winkel, bonnummer of product…', 'aria-label': 'Zoek bon' });
  let t;
  input.addEventListener('input', () => { clearTimeout(t); t = setTimeout(() => { state.q = input.value.trim(); sync(); load(); }, 250); });
  const st = h('select', { 'aria-label': 'Status' },
    h('option', { value: '' }, 'Alle bonnen'), h('option', { value: 'review', selected: state.status === 'review' ? true : null }, 'Alleen te controleren'));
  st.addEventListener('change', () => { state.status = st.value; sync(); load(); });
  mount('#search-bar', input, st);
}

function sync() {
  const q = new URLSearchParams(location.search);
  for (const k of ['q', 'status']) state[k] ? q.set(k, state[k]) : q.delete(k);
  history.replaceState(null, '', `${location.pathname}?${q}`);
}

async function load() {
  const f = filters.get();
  const q = new URLSearchParams();
  if (f.year) q.set('year', f.year);
  if (f.chain) q.set('chain', f.chain);
  if (state.q) q.set('q', state.q);
  if (state.status) q.set('status', state.status);
  if (!table) mount('#list', skeleton(8));
  try {
    const rows = await api(`/api/receipts?${q}`);
    const spent = rows.reduce((a, r) => a + r.spent, 0);
    mount('#summary',
      h('div', { class: 'kpi' }, h('div', { class: 'label' }, 'Bonnen'), h('div', { class: 'value' }, fmt.num(rows.length))),
      h('div', { class: 'kpi' }, h('div', { class: 'label' }, 'Uitgegeven'), h('div', { class: 'value' }, fmt.eur(spent))),
      h('div', { class: 'kpi' }, h('div', { class: 'label' }, 'Te controleren'),
        h('div', { class: 'value' }, fmt.num(rows.filter(r => r.quality !== null && r.quality < 1).length))));
    const columns = [
      { key: 'invoice_date', label: 'Datum', className: 'nowrap', render: r => h('a', { href: `/receipts/${r.id}` }, fmt.date(r.invoice_date)) },
      { key: 'store', label: 'Winkel', className: 'name', render: r => h('span', { class: 'row' }, storeBadge(r.chain), r.store || '–',
        h('span', { class: 'sub', style: { width: '100%' } }, SOURCES[r.source_type] || r.source_type)) },
      { key: 'lines', label: 'Regels', num: true, className: 'hide-mobile' },
      { key: 'bonus', label: 'Korting', num: true, className: 'hide-mobile', render: r => r.bonus ? fmt.eur(r.bonus) : '–' },
      { key: 'spent', label: 'Uitgegeven', num: true, render: r => h('span', {}, fmt.eur(r.spent),
        r.savings_stamps ? h('span', { class: 'sub' }, `+ ${fmt.eur(r.savings_stamps)} gespaard`) : null) },
      { key: 'status', label: 'Status', sort: r => (r.quality ?? 1), render: status },
    ];
    if (table) table.update(rows);
    else mount('#list', table = dataTable({ columns, rows, sortKey: 'invoice_date', pageSize: 40, className: 'stack-mobile',
      onRow: r => { location.href = `/receipts/${r.id}`; }, emptyText: 'Geen bonnen gevonden' }));
  } catch (err) {
    mount('#list', errorBox(err));
  }
}

const meta = await api('/api/meta').catch(() => ({ available_years: [], chains: [] }));
const draw = () => mount('#filters', filterBar({ years: meta.available_years, chains: meta.chains, onChange: () => { draw(); load(); } }));
draw();
renderSearch();
load();
