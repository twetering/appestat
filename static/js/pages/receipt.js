import { api, h, mount, fmt, dataTable, openCategoryPicker, setProductCategory, errorBox, skeleton, toast, modal, confirmDialog, storeBadge } from '../core.js';

const root = document.getElementById('page');
const id = +root.dataset.id;
const SOURCES = { ah_online: 'Albert Heijn online-factuur', ah_kassabon: 'Albert Heijn kassabon', plus_receipt: 'PLUS kassabon (foto)', ah_api: 'Albert Heijn app' };

function reconciliation(d) {
  const r = d.reconciliation;
  const line = (label, value, cls = '') => h('div', { class: `row ${cls}`, style: { justifyContent: 'space-between' } },
    h('span', {}, label), h('span', { class: 'num' }, fmt.eur(value)));
  const extras = Object.entries(r.extras).filter(([, v]) => Math.abs(v) >= 0.01);
  const ok = Math.abs(r.difference) < 0.01;
  return h('section', { class: 'card' },
    h('div', { class: 'card-head' }, h('h2', {}, 'Klopt de bon?'), h('span', { class: 'spacer' }),
      h('span', { class: `badge ${ok && (d.parse_quality_score ?? 1) >= 1 ? 'good' : 'warn'}` },
        ok && (d.parse_quality_score ?? 1) >= 1 ? 'Telt precies op' : 'Controleren')),
    h('div', { class: 'card-body stack' },
      line('Boodschappen (regels hieronder)', r.groceries),
      extras.map(([k, v]) => line(d.extra_labels[k] || k, v, 'muted')),
      h('div', { style: { borderTop: '1px solid var(--border)', paddingTop: '.5rem' } }, line('Betaald volgens de bon', r.paid)),
      !ok ? h('div', { class: 'notice' }, `Verschil van ${fmt.eur(r.difference)} tussen de regels en het totaal.`) : null,
      d.parse_notes ? h('div', { class: 'small muted' }, d.parse_notes) : null,
      h('div', { class: 'small muted' }, `Uitgegeven (zonder sparen): ${fmt.eur(r.spent)}`)));
}

function linesTable(d, reload) {
  const columns = [
    { key: 'display_name', label: 'Product', className: 'name', render: l => h('span', {},
      l.catalog_id ? h('a', { href: `/products/${l.catalog_id}` }, l.catalog_name || l.display_name) : (l.display_name || l.original_name),
      h('span', { class: 'sub' }, l.original_name !== (l.catalog_name || l.display_name) ? `bon: ${l.original_name}` : ''),
      l.suggested_category ? h('span', { class: 'badge brand' }, 'suggestie') : null,
      l.needs_review && !l.suggested_category ? h('span', { class: 'badge warn' }, 'controleren') : null) },
    { key: 'category', label: 'Categorie', render: l => {
      const btn = h('button', { class: 'cat-button', type: 'button' },
        h('span', { class: 't' }, l.category || 'Kies'), l.subcategory ? h('span', { class: 's' }, `› ${l.subcategory}`) : null,
        l.line_override ? h('span', { class: 'badge', title: 'Alleen voor deze regel aangepast' }, 'regel') : null);
      btn.addEventListener('click', () => openCategoryPicker(btn, {
        category: l.category, subcategory: l.subcategory,
        scopeLabel: l.catalog_id ? 'Geldt voor dit product op al je bonnen' : 'Geldt voor deze regel',
        onPick: async (c, s) => {
          if (l.catalog_id) await setProductCategory(l.catalog_id, c, s);
          else {
            await api(`/api/products/${l.id}/category`, { method: 'PUT', body: { category: c } });
            if (s) await api(`/api/products/${l.id}/subcategory`, { method: 'PUT', body: { subcategory: s } });
            toast('Categorie aangepast');
          }
          reload();
        },
      }));
      return btn;
    } },
    { key: 'quantity', label: 'Aantal', num: true, render: l => l.weight_kg ? `${fmt.num(l.weight_kg)} kg` : (l.pack_count > 1 ? `${l.quantity}× ${l.pack_count}` : l.quantity) },
    { key: 'price', label: 'Prijs', num: true, render: l => h('span', {}, fmt.eur(l.price),
      l.discount_amount ? h('span', { class: 'sub' }, `${fmt.eur(l.gross_price)} − ${fmt.eur(l.discount_amount)}`) : null) },
  ];
  return dataTable({ columns, rows: d.lines, className: 'stack-mobile', sortKey: null });
}

function extrasCard(d) {
  if (!d.extras.length) return null;
  return h('section', { class: 'card', style: { marginTop: '1rem' } },
    h('div', { class: 'card-head' }, h('h2', {}, 'Bijkomend en gespaard')),
    h('div', { class: 'card-body flush' }, dataTable({
      columns: [
        { key: 'description', label: 'Omschrijving' },
        { key: 'extra_type', label: 'Soort', render: e => h('span', { class: `badge ${e.extra_type === 'savings' ? 'good' : ''}` }, d.extra_labels[e.extra_type] || e.extra_type) },
        { key: 'quantity', label: 'Aantal', num: true },
        { key: 'amount', label: 'Bedrag', num: true, render: e => fmt.eur(e.amount) },
      ], rows: d.extras, sortKey: null,
    })));
}

async function load() {
  mount(root, skeleton(6, '2rem'));
  let d;
  try {
    d = await api(`/api/receipts/${id}`);
  } catch (err) {
    mount(root, h('div', { class: 'page-head' }, h('h1', {}, 'Bon niet gevonden')), errorBox(err),
      h('p', { style: { marginTop: '1rem' } }, h('a', { href: '/receipts' }, '← Terug naar bonnen')));
    return;
  }
  document.title = `Bon ${fmt.date(d.invoice_date)} · Appestat`;
  const validated = d.lines.length && d.lines.every(l => l.is_validated);
  mount(root,
    h('div', { class: 'page-head' },
      h('div', {},
        h('div', { class: 'crumbs' }, h('a', { href: '/receipts' }, 'Bonnen'), ' › ', d.invoice_number || d.filename),
        h('h1', { class: 'row' }, storeBadge(d.chain), d.store || 'Bon'),
        h('p', { class: 'sub' }, `${fmt.date(d.invoice_date)} · ${SOURCES[d.source_type] || d.source_type} · ${d.lines.length} regels`)),
      h('div', { class: 'row' },
        d.prev_id ? h('a', { class: 'btn small', href: `/receipts/${d.prev_id}`, title: 'Vorige bon' }, '←') : null,
        d.next_id ? h('a', { class: 'btn small', href: `/receipts/${d.next_id}`, title: 'Volgende bon' }, '→') : null,
        h('button', { class: 'btn small', onclick: async () => {
          const { text } = await api(`/api/receipts/${id}/text`);
          modal('Originele tekst', h('pre', {}, text || '(geen tekst opgeslagen)'));
        } }, 'Originele tekst'),
        h('button', { class: 'btn small', title: 'Lees de bon opnieuw in vanuit het originele bestand', onclick: async () => {
          if (!await confirmDialog('Opnieuw inlezen', 'De bon wordt opnieuw ingelezen uit het originele bestand. Categorieën blijven behouden.', 'Opnieuw inlezen')) return;
          try { await api(`/api/receipts/${id}/reparse`, { method: 'POST' }); toast('Opnieuw ingelezen'); load(); }
          catch (err) { toast(err.message, 'error'); }
        } }, 'Opnieuw inlezen'),
        h('button', { class: `btn small ${validated ? '' : 'primary'}`, onclick: async () => {
          await api(`/api/receipts/${id}/validate`, { method: 'POST', body: { validated: !validated } });
          toast(validated ? 'Markering verwijderd' : 'Bon gemarkeerd als gecontroleerd'); load();
        } }, validated ? '✓ Gecontroleerd' : 'Markeer als gecontroleerd'))),
    h('div', { class: 'kpis' },
      h('div', { class: 'kpi brand' }, h('div', { class: 'label' }, 'Betaald'), h('div', { class: 'value' }, fmt.eur(d.total_amount))),
      h('div', { class: 'kpi good' }, h('div', { class: 'label' }, 'Korting'), h('div', { class: 'value' }, fmt.eur(d.total_savings))),
      h('div', { class: 'kpi' }, h('div', { class: 'label' }, 'Uitgegeven'), h('div', { class: 'value' }, fmt.eur(d.reconciliation.spent)))),
    h('div', { class: 'grid grid-main' },
      h('section', { class: 'card' }, h('div', { class: 'card-head' }, h('h2', {}, 'Producten')), h('div', { class: 'card-body flush' }, linesTable(d, load))),
      h('div', {}, reconciliation(d), extrasCard(d))));
}

load();
