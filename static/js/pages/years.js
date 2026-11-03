import { api, h, mount, fmt, filters, filterBar, dataTable, chart, chartColors, errorBox, skeleton, MONTHS } from '../core.js';

async function load() {
  const f = filters.get();
  mount('#years-table', skeleton(5));
  try {
    const [meta, d] = await Promise.all([api('/api/meta'), api(`/api/years${f.chain ? `?chain=${encodeURIComponent(f.chain)}` : ''}`)]);
    mount('#filters', filterBar({ years: [], chains: meta.chains, showYear: false, onChange: load }));
    const yearLabel = y => h('span', {}, y.year, y.complete ? null : h('span', { class: 'badge warn', title: `Alleen ${y.months} maand(en) met data` }, ` ${y.months} mnd`));
    mount('#years-table', dataTable({
      columns: [
        { key: 'year', label: 'Jaar', render: yearLabel },
        { key: 'spent', label: 'Uitgegeven', num: true, render: y => h('strong', {}, fmt.eur(y.spent)) },
        { key: 'avg_per_month', label: 'Per maand', num: true, render: y => fmt.eur(y.avg_per_month) },
        { key: 'groceries', label: 'Boodschappen', num: true, className: 'hide-mobile', render: y => fmt.eur(y.groceries) },
        { key: 'extra_costs', label: 'Bijkomend', num: true, className: 'hide-mobile', render: y => fmt.eur(y.extra_costs) },
        { key: 'bonus', label: 'Korting', num: true, className: 'hide-mobile', render: y => fmt.eur(y.bonus) },
        { key: 'savings_stamps', label: 'Gespaard', num: true, className: 'hide-mobile', render: y => fmt.eur(y.savings_stamps) },
        { key: 'receipts', label: 'Bonnen', num: true, className: 'hide-mobile' },
      ], rows: d.years, sortKey: 'year', className: 'stack-mobile',
      onRow: y => { location.href = `/?year=${y.year}`; },
    }), h('p', { class: 'small muted', style: { padding: '.6rem 1rem' } },
      'Per maand = uitgegeven gedeeld door de kalendermaanden met data. Onvolledige jaren zijn gemarkeerd.'));

    const c = chartColors();
    const palette = [c.muted, c.warn, c.good, c.brand, c.bad];
    const years = Object.keys(d.monthly).sort();
    chart(document.getElementById('month-chart'), {
      type: 'line',
      data: { labels: MONTHS, datasets: years.map((y, i) => ({
        label: y, data: MONTHS.map((_, m) => d.monthly[y][String(m + 1).padStart(2, '0')] ?? null),
        borderColor: palette[(i + palette.length - years.length) % palette.length], backgroundColor: 'transparent',
        borderWidth: y === years[years.length - 1] ? 3 : 2, tension: .25, spanGaps: false,
      })) },
      options: { maintainAspectRatio: false, scales: { y: { ticks: { callback: v => fmt.eur0(v) } } },
        plugins: { tooltip: { callbacks: { label: ctx => `${ctx.dataset.label}: ${fmt.eur(ctx.parsed.y)}` } } } },
    });

    const complete = d.years.filter(y => y.complete).map(y => y.year);
    const [prev, last] = complete.slice(-2);
    const cols = [
      { key: 'name', label: 'Categorie', className: 'name', render: r => h('span', {}, r.icon, ' ', r.name) },
      ...d.years.map(y => ({ key: y.year, label: y.complete ? y.year : `${y.year}*`, num: true, sort: r => r.by_year[y.year] || 0,
        className: y.year === d.years[0].year && d.years.length > 3 ? 'hide-mobile' : '', render: r => fmt.eur0(r.by_year[y.year]) })),
      prev && last ? { key: 'change', label: `${prev}→${last}`, num: true, sort: r => change(r, prev, last) ?? -999,
        render: r => { const ch = change(r, prev, last); return ch === null ? '–' : h('span', { class: `badge ${ch > 10 ? 'warn' : ch < -10 ? 'good' : ''}` }, fmt.signedPct(ch)); } } : null,
    ].filter(Boolean);
    mount('#cat-table', dataTable({ columns: cols, rows: d.categories, sortKey: last || d.years[d.years.length - 1].year }),
      h('p', { class: 'small muted', style: { padding: '.6rem 1rem' } }, '* onvolledig jaar'));
  } catch (err) {
    mount('#years-table', errorBox(err));
  }
}

function change(r, a, b) {
  const va = r.by_year[a], vb = r.by_year[b];
  return va && vb ? (vb - va) / va * 100 : null;
}

await new Promise(r => (window.Chart ? r() : window.addEventListener('load', r, { once: true })));
load();
