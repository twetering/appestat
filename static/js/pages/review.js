import { api, h, mount, fmt, openCategoryPicker, setProductCategory, errorBox, skeleton, empty, toast, confirmDialog } from '../core.js';

let rows = [];

function reason(p) {
  if (p.suggested_category) return h('span', { class: 'badge brand' }, 'AI denkt anders');
  if (p.needs_review) return h('span', { class: 'badge warn' }, `AI twijfelt (${fmt.pct((p.category_confidence || 0) * 100, 0)})`);
  if (!p.category) return h('span', { class: 'badge bad' }, 'geen categorie');
  return h('span', { class: 'badge' }, 'op Overig');
}

function remove(p) {
  rows = rows.filter(r => r.id !== p.id);
  render();
}

function row(p) {
  const current = `${p.category || '–'}${p.subcategory ? ' › ' + p.subcategory : ''}`;
  const pickBtn = h('button', { class: 'btn small' }, 'Andere categorie…');
  pickBtn.addEventListener('click', () => openCategoryPicker(pickBtn, {
    category: p.category, subcategory: p.subcategory, scopeLabel: 'Geldt voor dit product op al je bonnen',
    onPick: async (c, s) => { await setProductCategory(p.id, c, s); remove(p); },
  }));
  return h('div', { class: 'card-body', style: { borderBottom: '1px solid var(--border)' } },
    h('div', { class: 'row', style: { justifyContent: 'space-between', alignItems: 'flex-start' } },
      h('div', { style: { minWidth: 0, flex: '1 1 300px' } },
        h('div', { class: 'row' }, h('a', { href: `/products/${p.id}`, style: { fontWeight: 600 } }, p.name), reason(p)),
        h('div', { class: 'small muted' }, `${p.purchases}× gekocht · ${fmt.eur(p.spent)}${p.family ? ' · ' + p.family : ''}`),
        h('div', { class: 'small', style: { marginTop: '.35rem' } },
          h('span', { class: 'muted' }, 'Nu: '), current,
          p.suggested_category ? [h('span', { class: 'muted' }, '  →  AI: '), h('strong', {},
            `${p.suggested_category}${p.suggested_subcategory ? ' › ' + p.suggested_subcategory : ''}`),
            h('span', { class: 'muted' }, ` (${fmt.pct((p.suggested_confidence || 0) * 100, 0)})`)] : null)),
      h('div', { class: 'row' },
        p.suggested_category ? h('button', { class: 'btn small primary', onclick: async () => {
          await api(`/api/catalog/${p.id}/suggestion/accept`, { method: 'POST' }); toast('Suggestie overgenomen'); remove(p);
        } }, 'AI-suggestie overnemen') : null,
        p.category ? h('button', { class: 'btn small', onclick: async () => {
          await api(`/api/catalog/${p.id}/confirm`, { method: 'POST' }); toast('Bevestigd'); remove(p);
        } }, p.suggested_category ? 'Huidige houden' : 'Klopt') : null,
        pickBtn)));
}

function render() {
  const suggestions = rows.filter(r => r.suggested_category && (r.suggested_confidence || 0) >= 0.95);
  mount('#actions', suggestions.length ? h('button', { class: 'btn primary', onclick: async () => {
    if (!await confirmDialog('Suggesties overnemen', `${suggestions.length} AI-suggesties met ≥95% zekerheid overnemen? Je kunt elke categorie later nog aanpassen.`, 'Overnemen')) return;
    const r = await api('/api/catalog/suggestions/accept-all', { method: 'POST', body: { min_confidence: 0.95 } });
    toast(`${r.accepted} suggesties overgenomen`); load();
  } }, `Alle sterke suggesties overnemen (${suggestions.length})`) : null);
  mount('#intro', h('p', { class: 'muted', style: { marginBottom: '1rem' } },
    rows.length ? `${rows.length} producten, samen ${fmt.eur(rows.reduce((a, r) => a + r.spent, 0))}. Een keuze hier geldt voor het product op al je bonnen, ook toekomstige.` : ''));
  mount('#list', rows.length ? rows.map(row) : empty('✅', 'Alles is gecontroleerd. Nieuwe producten verschijnen hier vanzelf als de AI twijfelt.'));
}

async function load() {
  mount('#list', skeleton(6, '3rem'));
  try {
    rows = await api('/api/catalog/review?limit=1000');
    render();
  } catch (err) {
    mount('#list', errorBox(err));
  }
}

load();
