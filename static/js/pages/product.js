import { api, h, mount, fmt, dataTable, categoryButton, setProductCategory, chart, chartColors, errorBox, skeleton, empty, toast, modal, confirmDialog, storeBadge } from '../core.js';

const root = document.getElementById('page');
const id = +root.dataset.id;

const kpi = (label, value, hint) => h('div', { class: 'kpi' }, h('div', { class: 'label' }, label),
  h('div', { class: 'value' }, value), hint ? h('div', { class: 'hint' }, hint) : null);

function header(p, reload) {
  const title = h('h1', {}, p.name);
  const rename = h('button', { class: 'btn small ghost', title: 'Naam wijzigen', onclick: async () => {
    const name = prompt('Nieuwe productnaam', p.name);
    if (!name || name === p.name) return;
    await api(`/api/catalog/${id}`, { method: 'PUT', body: { name } });
    toast('Naam aangepast'); reload();
  } }, '✎');
  const meta = [p.brand, p.family, p.is_private_label ? 'huismerk' : null].filter(Boolean).join(' · ');
  return h('div', { class: 'page-head' },
    h('div', {},
      h('div', { class: 'crumbs' }, h('a', { href: '/products' }, 'Producten'), ' › ', p.category || 'Zonder categorie'),
      h('div', { class: 'row' }, title, rename),
      h('p', { class: 'sub' }, meta || 'Product')),
    h('div', { class: 'row' },
      h('span', { class: 'small muted' }, 'Categorie'),
      categoryButton({ category: p.category, subcategory: p.subcategory, onPick: async (c, s) => { await setProductCategory(id, c, s); reload(); } })));
}

function suggestion(p, reload) {
  if (p.suggested_category) {
    return h('div', { class: 'notice brand', style: { marginBottom: '1rem' } },
      h('strong', {}, 'AI-suggestie: '), `${p.suggested_category}${p.suggested_subcategory ? ' › ' + p.suggested_subcategory : ''} `,
      h('span', { class: 'muted' }, `(${fmt.pct((p.suggested_confidence || 0) * 100, 0)} zeker; nu: ${p.category}${p.subcategory ? ' › ' + p.subcategory : ''}) `),
      h('button', { class: 'btn small primary', onclick: async () => { await api(`/api/catalog/${id}/suggestion/accept`, { method: 'POST' }); toast('Suggestie overgenomen'); reload(); } }, 'Overnemen'), ' ',
      h('button', { class: 'btn small', onclick: async () => { await api(`/api/catalog/${id}/confirm`, { method: 'POST' }); toast('Huidige categorie bevestigd'); reload(); } }, 'Huidige houden'));
  }
  if (p.needs_review) {
    return h('div', { class: 'notice', style: { marginBottom: '1rem' } },
      `De AI twijfelde over deze categorie (${fmt.pct((p.category_confidence || 0) * 100, 0)} zeker). Klopt hij? `,
      h('button', { class: 'btn small', onclick: async () => { await api(`/api/catalog/${id}/confirm`, { method: 'POST' }); toast('Bevestigd'); reload(); } }, 'Ja, klopt'));
  }
  return null;
}

function variantsCard(p) {
  const best = p.best_variant;
  return h('section', { class: 'card' },
    h('div', { class: 'card-head' }, h('h2', {}, 'Verpakkingen'), h('span', { class: 'spacer' }),
      best && p.variants.length > 1 ? h('span', { class: 'badge good' }, `Voordeligst: ${best.pack_count > 1 ? best.pack_count + '-pack' : 'per stuk'}${best.chain ? ' bij ' + best.chain : ''}`) : null),
    h('div', { class: 'card-body flush' }, dataTable({
      columns: [
        { key: 'pack_count', label: 'Verpakking', render: v => v.pack_count > 1 ? `${v.pack_count}-pack` : 'per stuk' },
        { key: 'chain', label: 'Winkel', render: v => storeBadge(v.chain) },
        { key: 'purchases', label: 'Keer', num: true },
        { key: 'spent', label: 'Uitgegeven', num: true, render: v => fmt.eur(v.spent) },
        { key: 'unit_price', label: 'Per stuk', num: true, render: v => h('span', {}, fmt.unit(v.unit_price),
          best && v.unit_price === best.unit_price && p.variants.length > 1 ? h('span', { class: 'sub' }, 'voordeligst') : null) },
      ], rows: p.variants, sortKey: 'pack_count', sortDir: 'asc',
    })));
}

function priceChart(p) {
  const canvas = h('canvas', { 'aria-label': 'Prijs per stuk door de tijd' });
  const card = h('section', { class: 'card' },
    h('div', { class: 'card-head' }, h('h2', {}, 'Prijs per stuk'), h('span', { class: 'spacer' }),
      h('span', { class: 'info', title: 'Betaalde prijs per stuk, inclusief bonus- en actiekorting.' }, 'i')),
    h('div', { class: 'card-body' }, h('div', { class: 'chart-box' }, canvas)));
  requestAnimationFrame(() => {
    const c = chartColors();
    const palette = [c.brand, c.warn, c.good, c.bad, c.muted];
    const packs = [...new Set(p.lines.map(l => l.pack_count))].sort((a, b) => a - b);
    chart(canvas, {
      type: 'scatter',
      data: { datasets: packs.map((pack, i) => ({
        label: pack > 1 ? `${pack}-pack` : 'per stuk', pointRadius: 4, backgroundColor: palette[i % palette.length],
        data: p.lines.filter(l => l.pack_count === pack && l.unit_price > 0).map(l => ({ x: new Date(String(l.date).replace(' ', 'T')).getTime(), y: l.unit_price })),
      })) },
      options: { maintainAspectRatio: false,
        scales: { x: { ticks: { callback: v => fmt.date(new Date(v).toISOString()) } }, y: { ticks: { callback: v => fmt.unit(v) } } },
        plugins: { tooltip: { callbacks: { label: ctx => `${ctx.dataset.label}: ${fmt.unit(ctx.parsed.y)} (${fmt.date(new Date(ctx.parsed.x).toISOString())})` } } } },
    });
  });
  return card;
}

function historyCard(p) {
  return h('section', { class: 'card' },
    h('div', { class: 'card-head' }, h('h2', {}, `Aankopen (${p.purchases})`)),
    h('div', { class: 'card-body flush' }, dataTable({
      columns: [
        { key: 'date', label: 'Datum', className: 'nowrap', render: l => h('a', { href: `/receipts/${l.invoice_id}` }, fmt.date(l.date)) },
        { key: 'store', label: 'Winkel', className: 'hide-mobile', render: l => h('span', { class: 'row' }, storeBadge(l.chain), l.store) },
        { key: 'original_name', label: 'Op de bon', render: l => h('span', {}, l.original_name, l.pack_estimated ? h('span', { class: 'sub', title: 'Verpakking geschat uit de prijs' }, `≈ ${l.pack_count}-pack`) : null) },
        { key: 'quantity', label: 'Aantal', num: true, render: l => l.pack_count > 1 ? `${l.quantity}× ${l.pack_count}` : l.quantity },
        { key: 'price', label: 'Betaald', num: true, render: l => h('span', {}, fmt.eur(l.price), l.discount_amount ? h('span', { class: 'sub' }, `−${fmt.eur(l.discount_amount)} korting`) : null) },
        { key: 'unit_price', label: 'Per stuk', num: true, className: 'hide-mobile', render: l => fmt.unit(l.unit_price) },
      ], rows: p.lines, sortKey: 'date', pageSize: 25, className: 'stack-mobile',
    })));
}

async function pickProduct(title, exclude) {
  return new Promise(resolve => {
    const results = h('div', { class: 'stack', style: { marginTop: '.75rem', maxHeight: '320px', overflowY: 'auto' } });
    const input = h('input', { type: 'search', placeholder: 'Zoek product…', style: { width: '100%' } });
    let close;
    let timer;
    input.addEventListener('input', () => {
      clearTimeout(timer);
      timer = setTimeout(async () => {
        const q = input.value.trim();
        if (q.length < 2) { mount(results); return; }
        const rows = (await api(`/api/catalog?q=${encodeURIComponent(q)}&limit=20`)).filter(r => r.id !== exclude);
        mount(results, rows.length ? rows.map(r => h('button', { class: 'btn', style: { justifyContent: 'space-between', width: '100%' },
          onclick: () => { close(); resolve(r); } }, h('span', {}, r.name), h('span', { class: 'muted small' }, fmt.eur(r.spent))))
          : h('p', { class: 'muted small' }, 'Niets gevonden'));
      }, 200);
    });
    close = modal(title, h('div', {}, input, results));
    setTimeout(() => input.focus(), 50);
  });
}

function aliasesCard(p, reload) {
  const rows = p.aliases.map(a => h('tr', {},
    h('td', {}, a.example_name, h('span', { class: 'sub' }, `${a.pack_count > 1 ? a.pack_count + '-pack · ' : ''}${a.chain || ''}${a.source === 'user' ? ' · door jou gekoppeld' : ''}`)),
    h('td', { class: 'num' },
      h('button', { class: 'btn small', title: 'Koppel deze schrijfwijze aan een ander product', onclick: async () => {
        const target = await pickProduct(`"${a.example_name}" verplaatsen naar…`, id);
        await api('/api/catalog/aliases/move', { method: 'POST', body: { alias: a.alias_key, target_id: target.id } });
        toast(`Verplaatst naar ${target.name}`); reload();
      } }, 'Verplaats'), ' ',
      p.aliases.length > 1 ? h('button', { class: 'btn small', title: 'Maak hier een apart product van', onclick: async () => {
        const name = prompt('Naam van het nieuwe product', a.example_name);
        if (!name) return;
        const r = await api('/api/catalog/aliases/move', { method: 'POST', body: { alias: a.alias_key, new_name: name } });
        toast('Apart product gemaakt'); location.href = `/products/${r.catalog_id}`;
      } }, 'Splits af') : null)));
  return h('section', { class: 'card' },
    h('div', { class: 'card-head' }, h('h2', {}, 'Schrijfwijzen op de bon'), h('span', { class: 'spacer' }),
      h('button', { class: 'btn small', onclick: async () => {
        const other = await pickProduct(`Samenvoegen met ${p.name}`, id);
        if (!await confirmDialog('Samenvoegen', `"${other.name}" wordt onderdeel van "${p.name}". Alle aankopen gaan mee.`, 'Samenvoegen')) return;
        await api('/api/catalog/merge', { method: 'POST', body: { keep_id: id, merge_ids: [other.id] } });
        toast('Samengevoegd'); reload();
      } }, 'Samenvoegen met…')),
    h('div', { class: 'card-body flush' }, h('div', { class: 'table-wrap' }, h('table', { class: 'data' }, h('tbody', {}, rows)))));
}

async function load() {
  mount(root, skeleton(6, '2rem'));
  let p;
  try {
    p = await api(`/api/catalog/${id}`);
  } catch (err) {
    mount(root, h('div', { class: 'page-head' }, h('h1', {}, 'Product niet gevonden')), errorBox(err),
      h('p', { style: { marginTop: '1rem' } }, h('a', { href: '/products' }, '← Terug naar producten')));
    return;
  }
  document.title = `${p.name} · Appestat`;
  const trend = p.unit_price_trend_pct;
  mount(root,
    header(p, load),
    suggestion(p, load),
    h('div', { class: 'kpis' },
      kpi('Uitgegeven', fmt.eur(p.spent), `${p.purchases} aankopen`),
      kpi('Aantal stuks', fmt.num(p.units), p.variants.length > 1 ? `${p.variants.length} verpakkingen` : null),
      kpi('Gemiddeld per stuk', fmt.unit(p.unit_price_avg), trend !== null ? `${fmt.signedPct(trend)} t.o.v. je eerste aankopen` : null),
      kpi('Per maand', fmt.eur(p.projection.monthly), `30 jaar: ${fmt.eur0(p.projection.thirty_years)}`),
      kpi('30 jaar incl. 5% rente', fmt.eur0(p.projection.thirty_years_invested), 'als je het elke maand had belegd')),
    h('div', { class: 'grid grid-2' }, variantsCard(p), priceChart(p)),
    h('div', { style: { marginTop: '1rem' } }, historyCard(p)),
    h('div', { style: { marginTop: '1rem' } }, aliasesCard(p, load)),
    h('p', { class: 'small muted', style: { marginTop: '1rem' } },
      `"Per maand" = uitgegeven gedeeld door ${p.projection_months} kalendermaanden met data; 30 jaar = per maand × 360; incl. 5% rente = hetzelfde bedrag elke maand belegd tegen 5% per jaar.`));
}

await new Promise(r => (window.Chart ? r() : window.addEventListener('load', r, { once: true })));
load();
