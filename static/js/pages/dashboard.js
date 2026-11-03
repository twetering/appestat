import { api, h, mount, fmt, filters, filterBar, chart, chartColors, eurTooltip, errorBox, skeleton, empty, storeBadge, dataTable } from '../core.js';

const kpi = (label, value, hint, cls = '', info = null) => h('div', { class: `kpi ${cls}` },
  h('div', { class: 'label' }, label, info ? h('span', { class: 'info', title: info }, 'i') : null),
  h('div', { class: 'value' }, value), hint ? h('div', { class: 'hint' }, hint) : null);

function periodLabel(o) {
  const s = o.summary;
  if (!s.first_date) return 'Nog geen bonnen in deze periode';
  const where = o.filters.chain ? ` bij ${o.filters.chain}` : '';
  return `${fmt.date(s.first_date)} t/m ${fmt.date(s.last_date)} · ${s.receipts} bonnen${where}`;
}

function renderKpis(o) {
  const s = o.summary;
  const extra = Object.entries(s.extra_costs_by_type).filter(([, v]) => Math.abs(v) >= 0.01).map(([k, v]) => `${{ deposit: 'statiegeld', delivery: 'bezorging', subscription: 'abonnement' }[k] || k} ${fmt.eur(v)}`).join(' · ');
  mount('#kpis',
    kpi('Uitgegeven', fmt.eur(s.spent), `${fmt.eur(s.avg_per_month)} per maand (${s.months} mnd)`, 'brand',
      'Wat je betaalde, min wat je spaarde (koopzegels, PLUSPunten). = boodschappen + bijkomende kosten.'),
    kpi('Boodschappen', fmt.eur(s.groceries), `${fmt.eur(s.avg_per_receipt)} per bon`),
    kpi('Bijkomende kosten', fmt.eur(s.extra_costs), extra || 'statiegeld, bezorging, abonnementen'),
    kpi('Korting gekregen', fmt.eur(s.bonus_savings), 'bonus, acties en kortingen', 'good'),
    kpi('Gespaard', fmt.eur(s.savings_stamps), 'koopzegels & spaarpunten', '',
      'Koopzegels en PLUSPunten betaal je bij de kassa, maar het is sparen: daarom telt het niet als uitgave.'));
}

function renderCharts(o) {
  const c = chartColors();
  const cats = o.categories.slice(0, 10);
  const rest = o.categories.slice(10).reduce((a, x) => a + x.total, 0);
  const labels = [...cats.map(x => x.name), ...(rest > 0 ? ['Overige categorieën'] : [])];
  const data = [...cats.map(x => x.total), ...(rest > 0 ? [rest] : [])];
  const colors = [...cats.map(x => x.color), ...(rest > 0 ? [c.muted] : [])];
  chart(document.getElementById('cat-chart'), {
    type: 'doughnut',
    data: { labels, datasets: [{ data, backgroundColor: colors, borderColor: c.surface, borderWidth: 2 }] },
    options: { maintainAspectRatio: false, cutout: '62%', plugins: {
      legend: { position: innerWidth < 1100 ? 'bottom' : 'right', labels: { boxWidth: 10, font: { size: 11 } } },
      tooltip: { callbacks: { label: ctx => `${ctx.label}: ${fmt.eur(ctx.parsed)}` } } } },
  });

  const allTime = !o.filters.year;
  const series = allTime ? o.yearly.map(y => [y.year, y.spent, y.complete]) : Object.entries(o.monthly).map(([k, v]) => [k, v, true]);
  document.getElementById('trend-title').textContent = allTime ? 'Per jaar' : 'Per maand';
  chart(document.getElementById('month-chart'), {
    type: 'bar',
    data: {
      labels: series.map(([k]) => allTime ? k : fmt.monthShort(k)),
      datasets: [{ label: 'Uitgegeven', data: series.map(([, v]) => v), borderRadius: 5,
        backgroundColor: series.map(([, , complete]) => complete ? c.brand : c.muted) }],
    },
    options: { maintainAspectRatio: false, plugins: { legend: { display: false }, tooltip: eurTooltip() },
      scales: { y: { ticks: { callback: v => fmt.eur0(v) } }, x: { grid: { display: false } } } },
  });
}

function renderCategories(o) {
  const max = Math.max(...o.categories.map(c => c.total), 1);
  const year = o.filters.year;
  const rows = o.categories.map(cat => {
    const subs = h('div', { class: 'sub-rows hidden' }, cat.subcategories.length
      ? cat.subcategories.map(s => h('div', { class: 'cat-row' },
        h('div', { class: 'name' }, h('span', { class: 't' }, s.name)),
        h('div', { class: 'bar-track' }, h('div', { class: 'bar-fill', style: { width: `${s.total / cat.total * 100}%`, background: cat.color } })),
        h('div', { class: 'amt' }, fmt.eur(s.total))))
      : h('div', { class: 'small muted', style: { padding: '.4rem 2.6rem' } }, 'Geen subcategorieën'),
      h('div', { style: { padding: '.3rem 2.6rem' } },
        h('a', { class: 'small', href: `/products?category=${encodeURIComponent(cat.name)}${year ? `&year=${year}` : ''}` }, 'Bekijk producten →')));
    const row = h('div', { class: 'cat-row', role: 'button', tabindex: '0', 'aria-expanded': 'false' },
      h('div', { class: 'name' }, h('span', {}, cat.icon), h('span', { class: 't' }, cat.name)),
      h('div', { class: 'bar-track' }, h('div', { class: 'bar-fill', style: { width: `${cat.total / max * 100}%`, background: cat.color } })),
      h('div', { class: 'amt' }, fmt.eur(cat.total), h('span', { class: 'pct' }, fmt.pct(cat.share))));
    const toggle = () => { const open = subs.classList.toggle('hidden'); row.setAttribute('aria-expanded', String(!open)); };
    row.addEventListener('click', toggle);
    row.addEventListener('keydown', e => { if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); toggle(); } });
    return [row, subs];
  });
  mount('#cat-list', rows.length ? rows : empty('🧾', 'Geen uitgaven in deze periode'));
}

function renderBudget(o) {
  const card = document.getElementById('budget-card');
  const b = o.budget;
  if (!b) {
    mount(card, h('div', { class: 'card-head' }, h('h2', {}, 'Budget')),
      h('div', { class: 'card-body small muted' }, o.filters.year && +o.filters.year !== new Date().getFullYear()
        ? 'Budget is er alleen voor het lopende jaar.' : h('span', {}, 'Nog geen budget ingesteld. ', h('a', { href: '/budget' }, 'Stel er een in →'))));
    return;
  }
  const pct = b.total_spent / b.total_budget * 100;
  const pace = b.expected_by_now / b.total_budget * 100;
  mount(card,
    h('div', { class: 'card-head' }, h('h2', {}, `Budget ${new Date().getFullYear()}`), h('span', { class: 'spacer' }),
      h('span', { class: `badge ${b.on_track ? 'good' : 'warn'}` }, b.on_track ? 'Binnen tempo' : 'Boven tempo')),
    h('div', { class: 'card-body' },
      h('div', { class: 'row', style: { justifyContent: 'space-between', marginBottom: '.4rem' } },
        h('strong', {}, `${fmt.eur(b.total_spent)} van ${fmt.eur(b.total_budget)}`), h('span', { class: 'muted small' }, fmt.pct(pct, 0))),
      h('div', { class: 'bar-track', style: { position: 'relative', height: '10px' } },
        h('div', { class: `bar-fill ${b.on_track ? '' : 'warn'}`, style: { width: `${Math.min(pct, 100)}%` } }),
        h('div', { title: 'Tempo: zoveel mocht je tot nu toe uitgeven', style: { position: 'absolute', top: '-3px', bottom: '-3px', left: `${Math.min(pace, 100)}%`, width: '2px', background: 'var(--text)' } })),
      h('p', { class: 'small muted', style: { marginTop: '.5rem' } },
        `Tot en met maand ${b.months_elapsed} mocht je ${fmt.eur(b.expected_by_now)} uitgeven. Verwachting voor het hele jaar: ${fmt.eur(b.projected_annual)}. `,
        h('a', { href: '/budget' }, 'Budget beheren →'))));
}

function renderStores(o) {
  const total = o.stores.reduce((a, s) => a + s.spent, 0) || 1;
  mount('#stores', o.stores.length ? dataTable({
    columns: [
      { key: 'store', label: 'Winkel', render: s => h('span', { class: 'row' }, storeBadge(s.chain), s.store) },
      { key: 'receipts', label: 'Bonnen', num: true },
      { key: 'spent', label: 'Uitgegeven', num: true, render: s => fmt.eur(s.spent) },
      { key: 'share', label: 'Aandeel', num: true, sort: s => s.spent, render: s => fmt.pct(s.spent / total * 100) },
    ], rows: o.stores, sortKey: 'spent', onRow: s => { location.href = `/receipts?q=${encodeURIComponent(s.store)}`; },
  }) : empty('🏪', 'Geen bonnen'));
}

function renderWeekdays(o) {
  const days = o.day_of_week.by_day;
  if (!days.length) { mount('#weekdays', empty('📅', 'Geen data')); return; }
  const max = Math.max(...days.map(d => d.total_spent));
  mount('#weekdays', days.map(d => h('div', { class: 'row', style: { marginBottom: '.35rem' } },
    h('span', { style: { width: '85px' }, class: 'small' }, d.day_name),
    h('div', { class: 'bar-track', style: { flex: 1 } }, h('div', { class: 'bar-fill', style: { width: `${d.total_spent / max * 100}%` } })),
    h('span', { class: 'small num', style: { width: '150px' } }, `${fmt.eur(d.total_spent)} · ${d.invoice_count}×`))),
    h('p', { class: 'small muted', style: { marginTop: '.5rem' } },
      'AH-online bonnen tellen op de bezorgdag. '));
}

function renderTopProducts(o) {
  mount('#top-products', dataTable({
    columns: [
      { key: 'name', label: 'Product', className: 'name', render: p => h('span', {}, p.catalog_id ? h('a', { href: `/products/${p.catalog_id}` }, p.name) : p.name,
        h('span', { class: 'sub' }, [p.category, p.subcategory].filter(Boolean).join(' › '))) },
      { key: 'purchases', label: 'Keer gekocht', num: true, className: 'hide-mobile' },
      { key: 'total', label: 'Uitgegeven', num: true, render: p => fmt.eur(p.total) },
      { key: 'monthly', label: 'Per maand', num: true, sort: p => p.projection.monthly, render: p => fmt.eur(p.projection.monthly), className: 'hide-mobile' },
      { key: 'thirty', label: '30 jaar', num: true, sort: p => p.projection.thirty_years, render: p => fmt.eur0(p.projection.thirty_years), className: 'hide-mobile' },
      { key: 'invested', label: '30 jaar incl. 5% rente', num: true, sort: p => p.projection.thirty_years_invested, render: p => fmt.eur0(p.projection.thirty_years_invested), className: 'hide-mobile' },
    ], rows: o.top_products, sortKey: 'total', className: 'stack-mobile',
    onRow: p => { if (p.catalog_id) location.href = `/products/${p.catalog_id}`; },
  }), h('p', { class: 'small muted', style: { padding: '.6rem 1rem' } },
    `"Per maand" = uitgegeven gedeeld door ${o.summary.months} kalendermaanden in deze periode; 30 jaar = per maand × 360; incl. 5% rente = wat het zou opleveren als je dat bedrag elke maand had belegd tegen 5% per jaar.`));
}

function renderPrices(p) {
  mount('#pack-savings', p.pack_savings.length ? [dataTable({
    columns: [
      { key: 'name', label: 'Product', className: 'name', render: x => h('a', { href: `/products/${x.id}` }, x.name) },
      { key: 'avg_unit_price', label: 'Jij betaalde', num: true, render: x => `${fmt.unit(x.avg_unit_price)}/st` },
      { key: 'best_unit_price', label: 'Voordeligst', num: true, render: x => h('span', {}, `${fmt.unit(x.best_unit_price)}/st`, h('span', { class: 'sub' }, `${x.best_pack}-pack`)) },
      { key: 'could_save', label: 'Besparing', num: true, render: x => h('strong', {}, fmt.eur(x.could_save)) },
    ], rows: p.pack_savings.slice(0, 6), sortKey: 'could_save',
  }), h('p', { class: 'small muted', style: { padding: '.6rem 1rem' } }, `Samen ${fmt.eur(p.pack_savings_total)} als je steeds de voordeligste verpakking had gekozen.`)]
    : empty('📦', 'Geen producten die je in meerdere verpakkingen koopt'));
  mount('#price-rising', p.rising.length ? dataTable({
    columns: [
      { key: 'name', label: 'Product', className: 'name', render: x => h('a', { href: `/products/${x.id}` }, x.name) },
      { key: 'first_unit_price', label: 'Was', num: true, render: x => fmt.unit(x.first_unit_price) },
      { key: 'last_unit_price', label: 'Nu', num: true, render: x => fmt.unit(x.last_unit_price) },
      { key: 'change_pct', label: 'Verschil', num: true, render: x => h('span', { class: 'badge warn' }, fmt.signedPct(x.change_pct)) },
    ], rows: p.rising.slice(0, 8), sortKey: 'change_pct',
  }) : empty('📉', 'Geen opvallende prijsstijgingen'));
}

async function load() {
  const f = filters.get();
  ['#kpis', '#cat-list', '#top-products', '#stores'].forEach(s => mount(s, skeleton(3)));
  try {
    const [o, prices] = await Promise.all([api(`/api/overview?${filters.query(f)}`), api(`/api/insights/prices${f.chain ? `?chain=${encodeURIComponent(f.chain)}` : ''}`)]);
    mount('#filters', filterBar({ years: o.available_years, chains: o.chains, onChange: load }));
    document.getElementById('period-label').textContent = periodLabel(o);
    mount('#notice', o.review_count ? h('div', { class: 'notice brand', style: { marginBottom: '1rem' } },
      `${o.review_count} producten wachten op een controle van hun categorie. `, h('a', { href: '/review' }, 'Naar review →')) : null);
    renderKpis(o);
    await new Promise(r => (window.Chart ? r() : window.addEventListener('load', r, { once: true })));
    renderCharts(o);
    renderCategories(o);
    renderBudget(o);
    renderStores(o);
    renderWeekdays(o);
    renderTopProducts(o);
    renderPrices(prices);
  } catch (err) {
    mount('#kpis', errorBox(err));
  }
}

load();
