import { api, h, mount, fmt, errorBox, skeleton, empty, toast, confirmDialog } from '../core.js';

const thisYear = new Date().getFullYear();
let year = +(new URLSearchParams(location.search).get('year') || thisYear);

const kpi = (label, value, hint, cls = '') => h('div', { class: `kpi ${cls}` }, h('div', { class: 'label' }, label),
  h('div', { class: 'value' }, value), hint ? h('div', { class: 'hint' }, hint) : null);

function yearSwitch() {
  mount('#year-switch', h('div', { class: 'chips' }, [thisYear - 1, thisYear, thisYear + 1].map(y =>
    h('button', { class: `chip ${y === year ? 'active' : ''}`, onclick: () => { year = y; history.replaceState(null, '', `?year=${y}`); load(); } }, y))));
}

function generateForm(d) {
  const pct = h('input', { type: 'number', min: '0', max: '50', step: '1', value: '10', style: { width: '4.5rem' }, 'aria-label': 'Besparing in procenten' });
  mount('#generate', h('div', { class: 'row' },
    h('span', { class: 'small muted' }, `Op basis van ${year - 1}, min`), pct, h('span', { class: 'small muted' }, '%'),
    h('button', { class: 'btn small', onclick: async () => {
      const ok = await confirmDialog('Budget genereren', `Alle categoriebudgetten voor ${year} worden ingesteld op je uitgaven in ${year - 1} min ${pct.value}%. Bestaande bedragen worden overschreven.`, 'Genereren');
      if (!ok) return;
      await api('/api/budget/categories/generate', { method: 'POST', body: { target_year: year, reduction_percentage: (+pct.value || 0) / 100 } });
      toast('Budget gegenereerd'); load();
    } }, 'Genereer')));
}

function categoryRow(c, d) {
  const target = c.target_amount || 0;
  const pct = target ? c.current_spent / target * 100 : null;
  const pace = d.months_elapsed && year === thisYear ? d.months_elapsed / 12 * 100 : (year < thisYear ? 100 : 0);
  const state = pct === null ? '' : pct > 100 ? 'bad' : pct > pace + 5 ? 'warn' : '';
  const input = h('input', { type: 'number', min: '0', step: '10', value: target ? Math.round(target) : '', placeholder: '–',
    style: { width: '6.5rem', textAlign: 'right' }, 'aria-label': `Budget ${c.category_name}` });
  input.addEventListener('change', async () => {
    await api('/api/budget/categories', { method: 'POST', body: { year, category_id: c.category_id, target_amount: +input.value || 0 } });
    toast(`Budget ${c.category_name}: ${fmt.eur0(+input.value || 0)}`); load();
  });
  return h('div', { class: 'card-body', style: { borderBottom: '1px solid var(--border)', padding: '.8rem 1.1rem' } },
    h('div', { class: 'row', style: { justifyContent: 'space-between' } },
      h('div', { class: 'row' }, h('span', {}, c.icon || '📋'), h('strong', {}, c.category_name)),
      h('div', { class: 'row small' },
        h('span', { class: 'muted' }, `${year - 1}: ${fmt.eur0(c.reference_spent)}`),
        h('span', {}, `${fmt.eur(c.current_spent)} van`), input)),
    target ? h('div', { class: 'bar-track', style: { position: 'relative', marginTop: '.5rem' } },
      h('div', { class: `bar-fill ${state}`, style: { width: `${Math.min(pct, 100)}%` } }),
      pace > 0 && pace < 100 ? h('div', { title: 'Tempo', style: { position: 'absolute', top: '-3px', bottom: '-3px', left: `${pace}%`, width: '2px', background: 'var(--text)' } }) : null) : null);
}

async function load() {
  yearSwitch();
  mount('#list', skeleton(8));
  try {
    const d = await api(`/api/budget/dashboard?year=${year}`);
    const hasBudget = d.total_budget > 0;
    const pace = d.expected_by_now;
    mount('#kpis',
      kpi(`Budget ${year}`, hasBudget ? fmt.eur0(d.total_budget) : '–', hasBudget ? `${fmt.eur0(d.total_budget / 12)} per maand` : 'nog niet ingesteld', 'brand'),
      kpi('Uitgegeven', fmt.eur(d.total_spent), hasBudget ? fmt.pct(d.total_spent / d.total_budget * 100, 0) + ' van budget' : null),
      year === thisYear ? kpi('Tempo', hasBudget ? fmt.eur0(pace) : '–', hasBudget ? `zoveel mocht je t/m maand ${d.months_elapsed} uitgeven` : null,
        d.on_track === false ? '' : 'good') : null,
      year >= thisYear ? kpi('Verwachting', fmt.eur0(d.projected_annual), 'voor het hele jaar bij dit tempo') : null,
      kpi(`Vorig jaar (${year - 1})`, fmt.eur0(d.reference_total)));
    const alerts = d.alerts || [];
    const details = h('ul', { class: 'hidden small', style: { margin: '.5rem 0 0 1.2rem' } }, alerts.map(a => h('li', {}, a.message, ' ',
      h('button', { class: 'btn small ghost', onclick: async () => { await api(`/api/budget/alerts/${a.id}/dismiss`, { method: 'POST' }); load(); } }, 'Verbergen'))));
    mount('#alerts', alerts.length ? h('div', { class: 'notice', style: { marginBottom: '1rem' } },
      h('div', { class: 'row' }, h('strong', {}, `${alerts.length} ${alerts.length === 1 ? 'categorie' : 'categorieën'} boven of dicht bij het budget`),
        h('button', { class: 'btn small ghost', onclick: e => { details.classList.toggle('hidden'); e.target.textContent = details.classList.contains('hidden') ? 'Toon' : 'Verberg'; } }, 'Toon')),
      details) : null);
    generateForm(d);
    const rows = d.categories.filter(c => c.category_name !== 'Overig' || c.current_spent || c.reference_spent);
    mount('#list', rows.length ? rows.map(c => categoryRow(c, d)) : empty('🎯', 'Nog geen uitgaven om een budget op te baseren'),
      h('p', { class: 'small muted', style: { padding: '.8rem 1.1rem' } },
        'Het streepje op de balk is het tempo: het deel van het jaar dat voorbij is. Wie links van het streepje blijft, houdt het jaarbudget.'));
  } catch (err) {
    mount('#list', errorBox(err));
  }
}

load();
