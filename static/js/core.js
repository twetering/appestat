// Appestat 3.0 — shared front-end helpers (ES module, no framework).
// Everything that renders data goes through h()/textContent: never innerHTML with data.

// ---------- Formatting (one Dutch format everywhere) ----------
const EUR = new Intl.NumberFormat('nl-NL', { style: 'currency', currency: 'EUR' });
const EUR0 = new Intl.NumberFormat('nl-NL', { style: 'currency', currency: 'EUR', maximumFractionDigits: 0 });
const NUM = new Intl.NumberFormat('nl-NL');
const MONTHS = ['jan', 'feb', 'mrt', 'apr', 'mei', 'jun', 'jul', 'aug', 'sep', 'okt', 'nov', 'dec'];

export const fmt = {
  eur: v => (v === null || v === undefined || Number.isNaN(+v)) ? '–' : EUR.format(+v),
  eur0: v => (v === null || v === undefined) ? '–' : EUR0.format(+v),
  num: v => (v === null || v === undefined) ? '–' : NUM.format(+v),
  pct: (v, d = 1) => (v === null || v === undefined) ? '–' : `${(+v).toLocaleString('nl-NL', { maximumFractionDigits: d })}%`,
  signedPct: v => (v === null || v === undefined) ? '–' : `${v > 0 ? '+' : ''}${(+v).toLocaleString('nl-NL', { maximumFractionDigits: 1 })}%`,
  unit: v => (v === null || v === undefined) ? '–' : `€ ${(+v).toLocaleString('nl-NL', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`,
  date: v => {
    if (!v) return '–';
    const d = new Date(String(v).replace(' ', 'T'));
    return Number.isNaN(d.getTime()) ? String(v) : `${d.getDate()} ${MONTHS[d.getMonth()]} ${d.getFullYear()}`;
  },
  month: key => { const [y, m] = String(key).split('-'); return `${MONTHS[+m - 1]} ${y}`; },
  monthShort: key => MONTHS[+String(key).slice(-2) - 1],
};
export { MONTHS };

// ---------- DOM ----------
export function h(tag, attrs = {}, ...children) {
  const el = document.createElement(tag);
  for (const [k, v] of Object.entries(attrs || {})) {
    if (v === null || v === undefined || v === false) continue;
    if (k === 'class') el.className = v;
    else if (k === 'style' && typeof v === 'object') Object.assign(el.style, v);
    else if (k.startsWith('on') && typeof v === 'function') el.addEventListener(k.slice(2), v);
    else if (k === 'dataset') Object.assign(el.dataset, v);
    else el.setAttribute(k, v === true ? '' : v);
  }
  for (const c of children.flat(Infinity)) {
    if (c === null || c === undefined || c === false) continue;
    el.append(c instanceof Node ? c : document.createTextNode(String(c)));
  }
  return el;
}
export const $ = (sel, root = document) => root.querySelector(sel);
export function mount(target, ...children) {
  const el = typeof target === 'string' ? $(target) : target;
  el.replaceChildren(...children.flat(Infinity).filter(c => c !== null && c !== undefined && c !== false));
  return el;
}

// ---------- API ----------
export async function api(path, { method = 'GET', body, form } = {}) {
  const opts = { method, headers: {} };
  if (form) opts.body = form;
  else if (body !== undefined) { opts.body = JSON.stringify(body); opts.headers['Content-Type'] = 'application/json'; }
  const res = await fetch(path, opts);
  let data = null;
  try { data = await res.json(); } catch { /* empty body */ }
  if (!res.ok) throw new Error((data && data.error) || `Fout ${res.status}`);
  return data;
}

// ---------- Feedback ----------
export function toast(message, type = '') {
  let box = $('.toasts');
  if (!box) { box = h('div', { class: 'toasts', role: 'status', 'aria-live': 'polite' }); document.body.append(box); }
  const t = h('div', { class: `toast ${type}` }, message);
  box.append(t);
  setTimeout(() => t.remove(), type === 'error' ? 6000 : 3200);
}

export function errorBox(err) {
  return h('div', { class: 'error-box' }, `Er ging iets mis: ${err.message || err}`);
}

export function empty(icon, text, extra) {
  return h('div', { class: 'empty' }, h('span', { class: 'icon' }, icon), text, extra || null);
}

export function skeleton(lines = 3, height = '1rem') {
  return h('div', { class: 'stack' }, Array.from({ length: lines }, () => h('div', { class: 'skeleton', style: { height } })));
}

export function modal(title, body, actions = []) {
  const close = () => backdrop.remove();
  const backdrop = h('div', { class: 'modal-backdrop', onclick: e => { if (e.target === backdrop) close(); } },
    h('div', { class: 'modal', role: 'dialog', 'aria-modal': 'true', 'aria-label': title },
      h('div', { class: 'card-head' }, h('h3', {}, title), h('span', { class: 'spacer' }),
        h('button', { class: 'btn ghost small', onclick: close, 'aria-label': 'Sluiten' }, '✕')),
      h('div', { class: 'card-body' }, body),
      actions.length ? h('div', { class: 'card-body row', style: { justifyContent: 'flex-end', paddingTop: 0 } }, actions) : null));
  document.body.append(backdrop);
  const onKey = e => { if (e.key === 'Escape') { close(); document.removeEventListener('keydown', onKey); } };
  document.addEventListener('keydown', onKey);
  return close;
}

export function confirmDialog(title, text, okLabel = 'Doorgaan', danger = false) {
  return new Promise(resolve => {
    let close;
    const done = v => { close(); resolve(v); };
    close = modal(title, h('p', {}, text), [
      h('button', { class: 'btn', onclick: () => done(false) }, 'Annuleren'),
      h('button', { class: `btn ${danger ? 'danger solid' : 'primary'}`, onclick: () => done(true) }, okLabel),
    ]);
  });
}

// ---------- Shared filters (period + store), kept in the URL and remembered ----------
export const filters = {
  get() {
    const q = new URLSearchParams(location.search);
    let saved = {};
    try { saved = JSON.parse(localStorage.getItem('appestat.filters') || '{}'); } catch { /* private mode */ }
    return {
      year: q.has('year') ? q.get('year') : (saved.year ?? String(new Date().getFullYear())),
      chain: q.has('chain') ? q.get('chain') : (saved.chain ?? ''),
    };
  },
  set(next) {
    const value = { ...filters.get(), ...next };
    try { localStorage.setItem('appestat.filters', JSON.stringify(value)); } catch { /* ignore */ }
    const q = new URLSearchParams(location.search);
    for (const [k, v] of Object.entries(value)) v ? q.set(k, v) : q.set(k, '');
    history.replaceState(null, '', `${location.pathname}?${q}`);
    return value;
  },
  query(f = filters.get()) {
    const q = new URLSearchParams();
    if (f.year) q.set('year', f.year);
    if (f.chain) q.set('chain', f.chain);
    return q.toString();
  },
};

export function filterBar({ years = [], chains = [], onChange, showYear = true, showChain = true, extra = null }) {
  const f = filters.get();
  const group = (label, options, key) => h('div', { class: 'group' },
    h('span', { class: 'lbl' }, label),
    h('div', { class: 'chips', role: 'group', 'aria-label': label }, options.map(([value, text]) =>
      h('button', {
        class: `chip ${String(f[key] ?? '') === String(value) ? 'active' : ''}`,
        'aria-pressed': String(String(f[key] ?? '') === String(value)),
        onclick: () => onChange(filters.set({ [key]: value })),
      }, text))));
  return h('div', { class: 'filters' },
    showYear ? group('Periode', [...years.map(y => [y, y]), ['', 'Alles']], 'year') : null,
    showChain && chains.length > 1 ? group('Winkel', [['', 'Alle'], ...chains.map(c => [c, c])], 'chain') : null,
    extra);
}

export function storeBadge(chain) {
  const cls = chain === 'PLUS' ? 'plus' : (chain === 'Albert Heijn' ? 'ah' : 'other');
  const label = chain === 'PLUS' ? 'PLUS' : (chain === 'Albert Heijn' ? 'AH' : (chain || '?'));
  return h('span', { class: `store-badge ${cls}`, title: chain || '' }, label);
}

// ---------- Category picker (one popover, product-level by default) ----------
let treeCache = null;
export async function categoryTree() {
  if (!treeCache) treeCache = api('/api/category-tree');
  return treeCache;
}

export function categoryButton({ category, subcategory, onPick, title = 'Categorie wijzigen' }) {
  const btn = h('button', { class: 'cat-button', title, type: 'button' },
    h('span', { class: 't' }, category || 'Kies categorie'),
    subcategory ? h('span', { class: 's' }, `› ${subcategory}`) : null);
  btn.addEventListener('click', e => { e.stopPropagation(); openCategoryPicker(btn, { category, subcategory, onPick }); });
  return btn;
}

export async function openCategoryPicker(anchor, { category, subcategory, onPick, scopeLabel = null }) {
  document.querySelectorAll('.popover').forEach(p => p.remove());
  const { tree } = await categoryTree();
  const input = h('input', { type: 'search', placeholder: 'Zoek categorie…', 'aria-label': 'Zoek categorie' });
  const list = h('div', { class: 'list', role: 'listbox' });
  const pop = h('div', { class: 'popover' }, input, list, scopeLabel ? h('div', { class: 'scope' }, scopeLabel) : null);
  let options = [];
  let focus = 0;

  const pick = (cat, sub) => { pop.remove(); cleanup(); onPick(cat, sub); };
  const render = () => {
    const q = input.value.trim().toLowerCase();
    options = [];
    const nodes = [];
    for (const [cat, subs] of Object.entries(tree)) {
      const catHit = cat.toLowerCase().includes(q);
      const subHits = subs.filter(s => !q || catHit || s.toLowerCase().includes(q));
      if (q && !catHit && !subHits.length) continue;
      if (!subs.length) {
        options.push([cat, null]);
        nodes.push(h('button', { class: `opt top ${cat === category ? 'current' : ''}`, onclick: () => pick(cat, null) }, cat));
        continue;
      }
      nodes.push(h('div', { class: 'group-label' }, cat));
      for (const s of subHits) {
        options.push([cat, s]);
        nodes.push(h('button', {
          class: `opt ${cat === category && s === subcategory ? 'current' : ''}`, onclick: () => pick(cat, s),
        }, s));
      }
    }
    focus = Math.min(focus, Math.max(options.length - 1, 0));
    mount(list, nodes.length ? nodes : h('div', { class: 'empty small' }, 'Geen categorie gevonden'));
    list.querySelectorAll('.opt')[focus]?.classList.add('focus');
  };
  input.addEventListener('input', () => { focus = 0; render(); });
  input.addEventListener('keydown', e => {
    const opts = list.querySelectorAll('.opt');
    if (e.key === 'ArrowDown') { focus = Math.min(focus + 1, opts.length - 1); e.preventDefault(); }
    else if (e.key === 'ArrowUp') { focus = Math.max(focus - 1, 0); e.preventDefault(); }
    else if (e.key === 'Enter' && options[focus]) { pick(...options[focus]); return; }
    else if (e.key === 'Escape') { pop.remove(); cleanup(); return; }
    else return;
    opts.forEach((o, i) => o.classList.toggle('focus', i === focus));
    opts[focus]?.scrollIntoView({ block: 'nearest' });
  });
  const outside = e => { if (!pop.contains(e.target) && e.target !== anchor) { pop.remove(); cleanup(); } };
  const cleanup = () => document.removeEventListener('mousedown', outside);
  document.addEventListener('mousedown', outside);

  document.body.append(pop);
  render();
  const r = anchor.getBoundingClientRect();
  const top = r.bottom + 6 + pop.offsetHeight > innerHeight ? Math.max(8, r.top - pop.offsetHeight - 6) : r.bottom + 6;
  pop.style.top = `${top}px`;
  pop.style.left = `${Math.min(Math.max(8, r.left), innerWidth - pop.offsetWidth - 8)}px`;
  input.focus();
}

export async function setProductCategory(catalogId, category, subcategory) {
  const res = await api(`/api/catalog/${catalogId}/category`, { method: 'PUT', body: { category, subcategory } });
  toast(`Categorie aangepast voor ${res.lines_updated} aankopen`);
  return res;
}

// ---------- Sortable table ----------
export function dataTable({ columns, rows, onRow, sortKey, sortDir = 'desc', pageSize = 0, className = '', emptyText = 'Geen resultaten' }) {
  let key = sortKey, dir = sortDir, page = 0;
  const wrap = h('div');
  const value = (row, col) => (col.sort ? col.sort(row) : row[col.key]);
  const render = () => {
    const sorted = key ? [...rows].sort((a, b) => {
      const col = columns.find(c => c.key === key);
      const va = value(a, col), vb = value(b, col);
      const cmp = (typeof va === 'number' && typeof vb === 'number') ? va - vb : String(va ?? '').localeCompare(String(vb ?? ''), 'nl');
      return dir === 'asc' ? cmp : -cmp;
    }) : rows;
    const pages = pageSize ? Math.max(1, Math.ceil(sorted.length / pageSize)) : 1;
    page = Math.min(page, pages - 1);
    const visible = pageSize ? sorted.slice(page * pageSize, (page + 1) * pageSize) : sorted;
    const head = h('tr', {}, columns.map(col => h('th', {
      class: `${col.num ? 'num' : ''} ${col.sortable !== false ? 'sortable' : ''} ${col.className || ''}`,
      'aria-sort': key === col.key ? (dir === 'asc' ? 'ascending' : 'descending') : null,
      onclick: col.sortable === false ? null : () => {
        dir = key === col.key && dir === 'desc' ? 'asc' : 'desc'; key = col.key; render();
      },
    }, col.label, key === col.key ? h('span', { class: 'arrow' }, dir === 'asc' ? ' ▲' : ' ▼') : null)));
    const body = visible.length ? visible.map(row => h('tr', {
      class: onRow ? 'clickable' : '', onclick: onRow ? e => { if (!e.target.closest('button, a, input, select')) onRow(row, e); } : null,
    }, columns.map(col => h('td', { class: `${col.num ? 'num' : ''} ${col.className || ''}` },
      col.render ? col.render(row) : (row[col.key] ?? '–')))))
      : [h('tr', {}, h('td', { colspan: columns.length }, empty('🔍', emptyText)))];
    mount(wrap,
      h('div', { class: 'table-wrap' }, h('table', { class: `data ${className}` }, h('thead', {}, head), h('tbody', {}, body))),
      pageSize && sorted.length > pageSize ? h('div', { class: 'pager' },
        h('span', {}, `${page * pageSize + 1}–${Math.min((page + 1) * pageSize, sorted.length)} van ${sorted.length}`),
        h('div', { class: 'row' },
          h('button', { class: 'btn small', disabled: page === 0, onclick: () => { page--; render(); } }, '← Vorige'),
          h('button', { class: 'btn small', disabled: page >= pages - 1, onclick: () => { page++; render(); } }, 'Volgende →')))
        : null);
  };
  render();
  wrap.update = newRows => { rows = newRows; page = 0; render(); };
  return wrap;
}

// ---------- Charts (Chart.js loaded by base.html) ----------
const charts = new WeakMap();
function cssVar(name) { return getComputedStyle(document.documentElement).getPropertyValue(name).trim(); }

export function chart(canvas, config) {
  charts.get(canvas)?.destroy();
  const Chart = window.Chart;
  if (!Chart) return null;
  Chart.defaults.font.family = cssVar('--font');
  Chart.defaults.color = cssVar('--muted');
  Chart.defaults.borderColor = cssVar('--border');
  const c = new Chart(canvas, config);
  charts.set(canvas, c);
  return c;
}

export const chartColors = () => ({ brand: cssVar('--brand'), good: cssVar('--good'), warn: cssVar('--warn'),
  bad: cssVar('--bad'), muted: cssVar('--faint'), surface: cssVar('--surface') });

export function eurTooltip() {
  return { callbacks: { label: ctx => `${ctx.dataset.label ? ctx.dataset.label + ': ' : ''}${fmt.eur(ctx.parsed.y ?? ctx.parsed)}` } };
}
