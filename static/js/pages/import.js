import { api, h, mount, fmt, dataTable, errorBox, empty, toast, storeBadge } from '../core.js';

const STATUS = { success: ['good', 'ingelezen'], skipped: ['', 'had je al'], error: ['bad', 'mislukt'] };

function showResults(results) {
  mount('#results', results.length ? dataTable({
    columns: [
      { key: 'file', label: 'Bestand', className: 'name' },
      { key: 'status', label: 'Resultaat', render: r => {
        const [cls, label] = STATUS[r.status] || ['', r.status];
        const warn = r.status === 'success' && r.parse_quality_score !== null && r.parse_quality_score < 1;
        return h('span', { class: `badge ${warn ? 'warn' : cls}`, title: r.reason || r.parse_notes || '' }, warn ? 'controleren' : label);
      } },
      { key: 'total_amount', label: 'Bedrag', num: true, render: r => r.total_amount ? fmt.eur(r.total_amount) : '–' },
      { key: 'link', label: '', sortable: false, render: r => r.invoice_id ? h('a', { href: `/receipts/${r.invoice_id}` }, 'Bekijk') : (r.reason || '') },
    ], rows: results, sortKey: null,
  }) : null);
}

async function upload(files) {
  if (!files.length) return;
  const form = new FormData();
  [...files].forEach(f => form.append('files', f));
  mount('#results', h('p', { class: 'muted' }, `${files.length} bestand(en) inlezen… foto's duren een paar seconden per stuk.`));
  try {
    const { results } = await api('/api/upload', { method: 'POST', form });
    const ok = results.filter(r => r.status === 'success').length;
    toast(`${ok} van ${results.length} bonnen ingelezen`);
    showResults(results);
    loadSide();
  } catch (err) {
    mount('#results', errorBox(err));
  }
}

function setupDropzone() {
  const zone = document.getElementById('dropzone');
  const input = document.getElementById('file-input');
  input.addEventListener('change', () => upload(input.files));
  ['dragenter', 'dragover'].forEach(ev => zone.addEventListener(ev, e => { e.preventDefault(); zone.classList.add('over'); }));
  ['dragleave', 'drop'].forEach(ev => zone.addEventListener(ev, e => { e.preventDefault(); zone.classList.remove('over'); }));
  zone.addEventListener('drop', e => upload(e.dataTransfer.files));
  document.getElementById('import-folder').addEventListener('click', async e => {
    e.target.disabled = true;
    mount('#results', h('p', { class: 'muted' }, 'Map doorzoeken…'));
    try {
      const r = await api('/api/invoices/import', { method: 'POST', body: {} });
      mount('#results', h('p', {}, `${r.imported} nieuw, ${r.skipped} al aanwezig, ${r.errors} mislukt${r.needs_review ? `, ${r.needs_review} te controleren` : ''}.`),
        (r.details || []).length ? h('ul', { class: 'small', style: { marginLeft: '1.2rem' } }, r.details.map(d => h('li', {}, `${d.file}: ${d.error || d.warning}`))) : null);
      loadSide();
    } catch (err) { mount('#results', errorBox(err)); }
    e.target.disabled = false;
  });
}

function setupManualForm() {
  const form = document.getElementById('manual-form');
  form.date.value = new Date().toISOString().slice(0, 10);
  api('/api/meta').then(meta => mount('#manual-chains', (meta.chains || [])
    .filter(c => !['Albert Heijn', 'PLUS'].includes(c)).map(c => h('option', { value: c }))));
  form.addEventListener('submit', async e => {
    e.preventDefault();
    const body = Object.fromEntries(new FormData(form));
    body.total = parseFloat(body.total);
    try {
      const r = await api('/api/receipts/manual', { method: 'POST', body });
      if (r.status === 'skipped') { toast(r.reason); return; }
      toast(`${body.chain} ${fmt.eur(body.total)} toegevoegd`);
      showResults([{ file: `${body.chain} · ${fmt.date(body.date)}`, ...r }]);
      form.total.value = ''; form.note.value = '';
    } catch (err) { toast(err.message, 'error'); }
  });
}

async function loadSide() {
  const [bad, status] = await Promise.all([api('/api/receipts?status=review'), api('/api/status')]);
  mount('#review-receipts', bad.length ? dataTable({
    columns: [
      { key: 'invoice_date', label: 'Datum', render: r => h('a', { href: `/receipts/${r.id}` }, fmt.date(r.invoice_date)) },
      { key: 'store', label: 'Winkel', render: r => h('span', { class: 'row' }, storeBadge(r.chain), r.store) },
      { key: 'parse_notes', label: 'Wat klopt er niet', className: 'small' },
    ], rows: bad, sortKey: 'invoice_date',
  }) : empty('✅', 'Alle bonnen tellen precies op tot het betaalde bedrag'));

  const sync = h('button', { class: 'btn', onclick: async e => {
    e.target.disabled = true; e.target.textContent = 'Bezig…';
    try {
      const r = await api('/api/catalog/sync', { method: 'POST', body: { merge: true } });
      toast(`${r.link.new_aliases} nieuwe schrijfwijzen gekoppeld`);
      mount('#catalog-usage', h('span', { class: 'muted' }, r.ai_usage));
    } catch (err) { toast(err.message, 'error'); }
    e.target.disabled = false; e.target.textContent = 'Catalogus bijwerken';
  } }, 'Catalogus bijwerken');
  const tree = await api('/api/category-tree');
  mount('#catalog',
    h('p', {}, 'Nieuwe producten worden na elke import automatisch herkend, aan bestaande producten gekoppeld en gecategoriseerd.'),
    h('p', {}, tree.ai_available ? h('span', { class: 'badge good' }, 'AI actief (OpenAI)') : h('span', { class: 'badge warn' }, 'Geen OpenAI-sleutel: alleen trefwoordregels')),
    status.review_products ? h('p', {}, h('a', { href: '/review' }, `${status.review_products} producten wachten op controle →`)) : null,
    sync, h('div', { id: 'catalog-usage' }));
}

async function loadAhApi() {
  const box = document.getElementById('ah-api');
  try {
    const s = await api('/api/ah/auth/status');
    mount(box,
      h('p', {}, 'Albert Heijn-app koppeling (onofficiële API). Niet nodig als je PDF-facturen of foto\'s gebruikt.'),
      s.authenticated
        ? h('div', { class: 'row' }, h('span', { class: 'badge good' }, 'gekoppeld'),
          h('button', { class: 'btn small', onclick: async () => { const r = await api('/api/ah/sync', { method: 'POST', body: {} }); toast(r.message || 'Gesynchroniseerd'); loadSide(); } }, 'Synchroniseren'),
          h('button', { class: 'btn small danger', onclick: async () => { await api('/api/ah/auth', { method: 'DELETE' }); loadAhApi(); } }, 'Ontkoppelen'))
        : h('div', { class: 'stack' },
          h('button', { class: 'btn small', onclick: async () => { const { url } = await api('/api/ah/auth/url'); window.open(url, '_blank', 'noopener'); } }, '1. Open AH-inlogpagina'),
          h('form', { class: 'row', onsubmit: async e => {
            e.preventDefault();
            try { await api('/api/ah/auth/exchange', { method: 'POST', body: { code: e.target.code.value } }); toast('Gekoppeld'); loadAhApi(); }
            catch (err) { toast(err.message, 'error'); }
          } }, h('input', { name: 'code', type: 'text', placeholder: '2. Plak appie://login-exit?code=…', 'aria-label': 'Code' }),
            h('button', { class: 'btn small', type: 'submit' }, 'Koppelen'))));
  } catch (err) {
    mount(box, errorBox(err));
  }
}

setupDropzone();
setupManualForm();
loadSide();
loadAhApi();
