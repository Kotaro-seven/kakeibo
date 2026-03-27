/* ================================================
   ザックリ家計簿 v2 - Firestore Subcollection Architecture
   ================================================
   Key change: Each record is an independent Firestore document
   in a subcollection, eliminating array-overwrite sync issues.

   Structure:
     households/{code}           → { budget, createdAt, updatedAt }
     households/{code}/records/  → { type, categoryId, ... }
   ================================================ */

(function () {
  'use strict';

  /* ---- Firebase ---- */
  const firebaseConfig = {
    apiKey: "AIzaSyBPJcEzV2LD8z15d8HXCWV7Px52Quh2rWo",
    authDomain: "zaku-kake.firebaseapp.com",
    projectId: "zaku-kake",
    storageBucket: "zaku-kake.firebasestorage.app",
    messagingSenderId: "912775929665",
    appId: "1:912775929665:web:3b1932902033da9521cbc0"
  };

  firebase.initializeApp(firebaseConfig);
  const db = firebase.firestore();

  // Enable offline persistence (silent fail OK)
  db.enablePersistence({ synchronizeTabs: true }).catch(() => {});

  /* ---- Categories ---- */
  const EXPENSE_CATEGORIES = [
    { id: 'food',          emoji: '🍔', label: '食費',   color: '#fb923c' },
    { id: 'supermarket',   emoji: '🛒', label: 'スーパー', color: '#4ade80' },
    { id: 'eating_out',    emoji: '🍽️', label: '外食',   color: '#f97316' },
    { id: 'drink',         emoji: '🍺', label: '飲み代', color: '#fbbf24' },
    { id: 'housing',       emoji: '🏠', label: '家賃',   color: '#60a5fa' },
    { id: 'transport',     emoji: '🚃', label: '交通費', color: '#34d399' },
    { id: 'entertainment', emoji: '🎮', label: '娯楽',   color: '#a855f7' },
    { id: 'clothing',      emoji: '👕', label: '衣服',   color: '#f472b6' },
    { id: 'medical',       emoji: '💊', label: '医療',   color: '#f87171' },
    { id: 'telecom',       emoji: '📱', label: '通信費', color: '#38bdf8' },
    { id: 'daily',         emoji: '🧴', label: '日用品', color: '#a3e635' },
    { id: 'other',         emoji: '🔧', label: 'その他', color: '#94a3b8' },
  ];

  const INCOME_CATEGORIES = [
    { id: 'salary',     emoji: '💼', label: '給料',   color: '#10b981' },
    { id: 'bonus',      emoji: '🎁', label: 'ボーナス', color: '#fbbf24' },
    { id: 'sidejob',    emoji: '💻', label: '副業',   color: '#60a5fa' },
    { id: 'investment', emoji: '📈', label: '投資',   color: '#a855f7' },
    { id: 'refund',     emoji: '🔄', label: '返金',   color: '#38bdf8' },
    { id: 'other_in',   emoji: '💰', label: 'その他', color: '#94a3b8' },
  ];

  /* ---- State ---- */
  let state = {
    records: new Map(),          // id → record object
    budget: 0,
    selectedCategory: null,
    entryType: 'expense',
    dashboardMonth: new Date(),
    historyMonth: new Date(),
    userCode: null,
  };

  let unsubMeta = null;
  let unsubRecords = null;

  /* ---- Helpers ---- */
  const $ = (s) => document.querySelector(s);
  const $$ = (s) => document.querySelectorAll(s);
  const parseAmount = (s) => Number(String(s).replace(/[^\d]/g, '')) || 0;

  function escapeHTML(str) {
    const d = document.createElement('div');
    d.textContent = str;
    return d.innerHTML;
  }

  function todayStr() {
    const d = new Date();
    return `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, '0')}-${String(d.getDate()).padStart(2, '0')}`;
  }

  function getRecordsArray() {
    return Array.from(state.records.values());
  }

  /* ---- LocalStorage (user code only) ---- */
  const CODE_KEY = 'zakkuri_v2_code';
  const getSavedCode = () => localStorage.getItem(CODE_KEY);
  const setSavedCode = (c) => localStorage.setItem(CODE_KEY, c);

  /* ---- Code Generation ---- */
  function generateCode() {
    const chars = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789';
    let code = '';
    for (let i = 0; i < 6; i++) code += chars[Math.floor(Math.random() * chars.length)];
    return code;
  }

  /* ---- Firestore Refs ---- */
  function getHouseholdRef() {
    return db.collection('households').doc(state.userCode);
  }

  function getRecordsRef() {
    return getHouseholdRef().collection('records');
  }

  /* ---- Sync Indicator ---- */
  function setSyncStatus(status) {
    const el = $('#sync-indicator');
    if (!el) return;
    el.className = 'sync-indicator ' + status;
    el.title = status === 'synced' ? '同期済み'
             : status === 'syncing' ? '同期中...'
             : '接続エラー';
  }

  /* ---- Firestore: Meta (budget) ---- */
  async function saveBudget(val) {
    if (!state.userCode) return;
    setSyncStatus('syncing');
    try {
      await getHouseholdRef().set({
        budget: val,
        updatedAt: firebase.firestore.FieldValue.serverTimestamp()
      }, { merge: true });
      setSyncStatus('synced');
    } catch (e) {
      console.error('Budget save error:', e);
      setSyncStatus('error');
    }
  }

  function startMetaListener() {
    if (unsubMeta) unsubMeta();
    unsubMeta = getHouseholdRef().onSnapshot((doc) => {
      if (doc.exists) {
        const data = doc.data();
        if (data.budget !== undefined) {
          state.budget = data.budget;
          updateBudgetBar();
        }
      }
      setSyncStatus('synced');
    }, (err) => {
      console.error('Meta listener error:', err);
      setSyncStatus('error');
    });
  }

  /* ---- Firestore: Records (subcollection) ---- */
  async function addRecordToFirestore(record) {
    setSyncStatus('syncing');
    try {
      await getRecordsRef().doc(record.id).set(record);
      setSyncStatus('synced');
    } catch (e) {
      console.error('Record add error:', e);
      setSyncStatus('error');
    }
  }

  async function deleteRecordFromFirestore(id) {
    setSyncStatus('syncing');
    try {
      await getRecordsRef().doc(id).delete();
      setSyncStatus('synced');
    } catch (e) {
      console.error('Record delete error:', e);
      setSyncStatus('error');
    }
  }

  function startRecordsListener() {
    if (unsubRecords) unsubRecords();

    unsubRecords = getRecordsRef().onSnapshot((snapshot) => {
      // Process only the changes (added / modified / removed)
      snapshot.docChanges().forEach((change) => {
        const data = { id: change.doc.id, ...change.doc.data() };
        if (change.type === 'added' || change.type === 'modified') {
          state.records.set(change.doc.id, data);
        } else if (change.type === 'removed') {
          state.records.delete(change.doc.id);
        }
      });
      updateAll();
      setSyncStatus('synced');
    }, (err) => {
      console.error('Records listener error:', err);
      setSyncStatus('error');
    });
  }

  /* ---- Init ---- */
  function init() {
    const saved = getSavedCode();
    if (saved) {
      state.userCode = saved;
      startApp();
    } else {
      showSetupModal();
    }
  }

  function startApp() {
    $('#date-input').value = todayStr();
    renderCategories();
    bindEvents();
    switchTab('input');
    startMetaListener();
    startRecordsListener();
    updateBudgetBar();
    renderRecentItems();
    if ($('#user-code-text')) {
      $('#user-code-text').textContent = state.userCode;
    }
  }

  /* ---- Setup Modal ---- */
  function showSetupModal() {
    $('#setup-modal').classList.add('show');

    $('#setup-new-btn').addEventListener('click', async () => {
      const code = generateCode();
      state.userCode = code;
      setSavedCode(code);
      await getHouseholdRef().set({
        budget: 0,
        createdAt: firebase.firestore.FieldValue.serverTimestamp(),
        updatedAt: firebase.firestore.FieldValue.serverTimestamp()
      });
      $('#setup-modal').classList.remove('show');
      startApp();
      showToast(`🎉 合言葉: ${code}`);
    });

    $('#setup-join-btn').addEventListener('click', async () => {
      const code = $('#setup-code-input').value.trim().toUpperCase();
      if (code.length !== 6) {
        showToast('⚠️ 6桁のコードを入力してください');
        return;
      }
      const ref = db.collection('households').doc(code);
      const snap = await ref.get();
      if (!snap.exists) {
        showToast('⚠️ このコードのデータが見つかりません');
        return;
      }
      state.userCode = code;
      setSavedCode(code);
      $('#setup-modal').classList.remove('show');
      startApp();
      showToast('✅ データを引き継ぎました！');
    });

    $('#setup-code-input').addEventListener('keydown', (e) => {
      if (e.key === 'Enter') $('#setup-join-btn').click();
    });
  }

  /* ---- Categories ---- */
  function getCategories() {
    return state.entryType === 'expense' ? EXPENSE_CATEGORIES : INCOME_CATEGORIES;
  }

  function renderCategories() {
    const grid = $('#category-grid');
    const cats = getCategories();
    grid.innerHTML = cats.map(c => `
      <button class="category-btn ${state.selectedCategory === c.id ? 'active' : ''}"
              data-id="${c.id}" id="cat-${c.id}">
        <span class="category-emoji">${c.emoji}</span>
        <span class="category-label">${c.label}</span>
      </button>
    `).join('');

    grid.querySelectorAll('.category-btn').forEach(btn => {
      btn.addEventListener('click', () => selectCategory(btn.dataset.id));
    });
  }

  function selectCategory(id) {
    state.selectedCategory = id;
    $$('.category-btn').forEach(b => b.classList.toggle('active', b.dataset.id === id));
    validateForm();
  }

  /* ---- Events ---- */
  function bindEvents() {
    // Tabs
    $$('.tab-btn').forEach(b => b.addEventListener('click', () => switchTab(b.dataset.tab)));

    // Type toggle
    $$('.type-btn').forEach(b => b.addEventListener('click', () => switchType(b.dataset.type)));

    // Amount
    const amountInput = $('#amount-input');
    amountInput.addEventListener('input', () => {
      let v = amountInput.value.replace(/[^\d]/g, '');
      if (v.length > 10) v = v.slice(0, 10);
      amountInput.value = v ? Number(v).toLocaleString() : '';
      validateForm();
    });

    // Quick amounts
    $$('.quick-btn').forEach(b => {
      b.addEventListener('click', () => {
        const cur = parseAmount(amountInput.value) || 0;
        amountInput.value = (cur + Number(b.dataset.amount)).toLocaleString();
        validateForm();
      });
    });

    // Save
    $('#save-btn').addEventListener('click', saveRecord);

    // Settings
    $('#settings-btn').addEventListener('click', openSettings);
    $('#close-settings').addEventListener('click', closeSettings);
    $('#save-settings').addEventListener('click', saveSettings);

    // Budget presets
    $$('.preset-btn').forEach(b => {
      b.addEventListener('click', () => {
        const input = $('#budget-input');
        input.value = Number(b.dataset.budget).toLocaleString();
        input.focus();
      });
    });

    // Inline budget edit
    $('#budget-total-display').addEventListener('click', startInlineBudgetEdit);
    const inlineInput = $('#budget-inline-input');
    inlineInput.addEventListener('keydown', (e) => {
      if (e.key === 'Enter') finishInlineBudgetEdit();
      if (e.key === 'Escape') cancelInlineBudgetEdit();
    });
    inlineInput.addEventListener('blur', finishInlineBudgetEdit);
    inlineInput.addEventListener('input', () => {
      let v = inlineInput.value.replace(/[^\d]/g, '');
      if (v.length > 10) v = v.slice(0, 10);
      inlineInput.value = v ? Number(v).toLocaleString() : '';
    });

    // Copy code
    $('#copy-code-btn').addEventListener('click', () => {
      navigator.clipboard.writeText(state.userCode).then(() => {
        showToast('📋 コードをコピーしました');
      }).catch(() => {
        showToast(`合言葉: ${state.userCode}`);
      });
    });

    // Export & Reset
    $('#export-btn').addEventListener('click', exportData);
    $('#reset-btn').addEventListener('click', () => {
      showConfirm('本当に全てのデータを削除しますか？\nこの操作は元に戻せません。', async () => {
        // Delete all records from Firestore
        const records = getRecordsArray();
        const batch = db.batch();
        records.forEach(r => batch.delete(getRecordsRef().doc(r.id)));
        await batch.commit();
        state.budget = 0;
        await saveBudget(0);
        state.records.clear();
        updateAll();
        closeSettings();
        showToast('🗑️ データをリセットしました');
      });
    });

    // Dashboard month nav
    $('#prev-month').addEventListener('click', () => navigateDashboardMonth(-1));
    $('#next-month').addEventListener('click', () => navigateDashboardMonth(1));

    // History month nav
    $('#hist-prev-month').addEventListener('click', () => navigateHistoryMonth(-1));
    $('#hist-next-month').addEventListener('click', () => navigateHistoryMonth(1));

    // Confirm
    $('#confirm-cancel').addEventListener('click', closeConfirm);
  }

  /* ---- Tab ---- */
  function switchTab(tab) {
    $$('.tab-btn').forEach(b => b.classList.toggle('active', b.dataset.tab === tab));
    $$('.tab-content').forEach(c => c.classList.remove('active'));
    $(`#tab-${tab}`).classList.add('active');
    if (tab === 'dashboard') updateDashboard();
    if (tab === 'history') updateHistory();
  }

  /* ---- Type ---- */
  function switchType(type) {
    state.entryType = type;
    state.selectedCategory = null;
    $$('.type-btn').forEach(b => b.classList.toggle('active', b.dataset.type === type));
    renderCategories();
    validateForm();
  }

  /* ---- Form ---- */
  function validateForm() {
    const amount = parseAmount($('#amount-input').value);
    $('#save-btn').disabled = !(state.selectedCategory && amount > 0);
  }

  /* ---- Save Record ---- */
  function saveRecord() {
    const amount = parseAmount($('#amount-input').value);
    if (!amount || !state.selectedCategory) return;

    const cats = getCategories();
    const cat = cats.find(c => c.id === state.selectedCategory);

    // Use selected date or today
    const dateVal = $('#date-input').value || todayStr();
    const dateObj = new Date(dateVal + 'T12:00:00');

    const record = {
      id: Date.now().toString(36) + Math.random().toString(36).slice(2, 6),
      type: state.entryType,
      categoryId: cat.id,
      emoji: cat.emoji,
      label: cat.label,
      color: cat.color,
      amount: amount,
      memo: $('#memo-input').value.trim(),
      date: dateObj.toISOString(),
      createdAt: new Date().toISOString(),
    };

    // Optimistic update: add to local state immediately
    state.records.set(record.id, record);
    updateAll();

    // Save to Firestore
    addRecordToFirestore(record);

    // Reset form
    $('#amount-input').value = '';
    $('#memo-input').value = '';
    $('#date-input').value = todayStr();
    state.selectedCategory = null;
    renderCategories();
    validateForm();

    const emoji = state.entryType === 'expense' ? '💸' : '💰';
    showToast(`${emoji} ¥${amount.toLocaleString()} を記録しました`);

    // Button feedback
    const btn = $('#save-btn');
    btn.style.transform = 'scale(0.95)';
    setTimeout(() => { btn.style.transform = ''; }, 150);
  }

  /* ---- Recent Items ---- */
  function renderRecentItems() {
    const list = $('#recent-list');
    const recent = getRecordsArray()
      .sort((a, b) => new Date(b.createdAt || b.date) - new Date(a.createdAt || a.date))
      .slice(0, 5);

    if (recent.length === 0) {
      list.innerHTML = '<div class="empty-state"><span class="empty-icon">📝</span><p>最初の記録をつけてみよう</p></div>';
      return;
    }

    list.innerHTML = recent.map(r => {
      const d = new Date(r.date);
      const timeStr = `${d.getMonth() + 1}/${d.getDate()} ${d.getHours()}:${String(d.getMinutes()).padStart(2, '0')}`;
      const sign = r.type === 'expense' ? '-' : '+';
      return `
        <div class="recent-item">
          <span class="recent-emoji">${r.emoji}</span>
          <div class="recent-info">
            <div class="recent-category">${r.label}</div>
            ${r.memo ? `<div class="recent-memo">${escapeHTML(r.memo)}</div>` : ''}
          </div>
          <span class="recent-amount ${r.type}">${sign}¥${r.amount.toLocaleString()}</span>
          <span class="recent-time">${timeStr}</span>
        </div>
      `;
    }).join('');
  }

  /* ---- Budget Bar ---- */
  function updateBudgetBar() {
    const now = new Date();
    const monthRecs = getMonthRecords(now.getFullYear(), now.getMonth());
    const spent = monthRecs.filter(r => r.type === 'expense').reduce((s, r) => s + r.amount, 0);

    const remaining = state.budget > 0 ? state.budget - spent : 0;
    const pct = state.budget > 0 ? Math.min((spent / state.budget) * 100, 100) : 0;

    const spentEl = $('#budget-spent');
    const totalEl = $('#budget-total-display');
    const progressEl = $('#budget-progress');
    const remainEl = $('#budget-remaining');
    const pctEl = $('#budget-pct');

    spentEl.textContent = `¥${spent.toLocaleString()}`;

    if (state.budget <= 0) {
      totalEl.textContent = '未設定 ✎';
      progressEl.style.width = '0%';
      progressEl.className = 'progress-fill';
      spentEl.className = 'budget-spent';
      remainEl.textContent = '';
      pctEl.textContent = 'タップして予算を設定';
    } else {
      totalEl.textContent = `¥${state.budget.toLocaleString()}`;
      if (pct >= 90) {
        spentEl.className = 'budget-spent danger';
        progressEl.className = 'progress-fill danger';
      } else if (pct >= 70) {
        spentEl.className = 'budget-spent warning';
        progressEl.className = 'progress-fill warning';
      } else {
        spentEl.className = 'budget-spent';
        progressEl.className = 'progress-fill';
      }
      progressEl.style.width = pct + '%';
      remainEl.textContent = `残り ¥${Math.max(remaining, 0).toLocaleString()}`;
      pctEl.textContent = `${pct.toFixed(0)}% 消化`;
    }
  }

  /* ---- Inline Budget Edit ---- */
  function startInlineBudgetEdit() {
    $('#budget-total-display').style.display = 'none';
    $('#budget-total-edit').style.display = 'flex';
    const input = $('#budget-inline-input');
    input.value = state.budget > 0 ? state.budget.toLocaleString() : '';
    input.focus();
    input.select();
  }

  function finishInlineBudgetEdit() {
    const editEl = $('#budget-total-edit');
    if (editEl.style.display === 'none') return;
    const val = parseAmount($('#budget-inline-input').value);
    state.budget = val;
    saveBudget(val);
    $('#budget-total-display').style.display = '';
    editEl.style.display = 'none';
    updateBudgetBar();
    if (val > 0) showToast(`✅ 予算を ¥${val.toLocaleString()} に設定しました`);
  }

  function cancelInlineBudgetEdit() {
    $('#budget-total-display').style.display = '';
    $('#budget-total-edit').style.display = 'none';
  }

  /* ---- Get month records ---- */
  function getMonthRecords(year, month) {
    return getRecordsArray().filter(r => {
      const d = new Date(r.date);
      return d.getFullYear() === year && d.getMonth() === month;
    }).sort((a, b) => new Date(b.date) - new Date(a.date));
  }

  /* ---- Dashboard ---- */
  function updateDashboard() {
    const d = state.dashboardMonth;
    const y = d.getFullYear(), m = d.getMonth();
    $('#dashboard-month').textContent = `${y}年${m + 1}月`;

    const records = getMonthRecords(y, m);
    const income = records.filter(r => r.type === 'income').reduce((s, r) => s + r.amount, 0);
    const expense = records.filter(r => r.type === 'expense').reduce((s, r) => s + r.amount, 0);
    const balance = income - expense;

    $('#total-income').textContent = `¥${income.toLocaleString()}`;
    $('#total-expense').textContent = `¥${expense.toLocaleString()}`;
    $('#total-balance').textContent = `${balance >= 0 ? '+' : ''}¥${balance.toLocaleString()}`;

    drawPieChart(records.filter(r => r.type === 'expense'));
    drawBarChart(records.filter(r => r.type === 'expense'), y, m);
    drawScore(expense);
  }

  function navigateDashboardMonth(dir) {
    const d = state.dashboardMonth;
    state.dashboardMonth = new Date(d.getFullYear(), d.getMonth() + dir, 1);
    updateDashboard();
  }

  /* ---- Pie Chart (dark theme) ---- */
  function drawPieChart(expenses) {
    const canvas = $('#pie-chart');
    const ctx = canvas.getContext('2d');
    const dpr = window.devicePixelRatio || 1;
    const w = 280, h = 280;
    canvas.width = w * dpr;
    canvas.height = h * dpr;
    canvas.style.width = '200px';
    canvas.style.height = '200px';
    ctx.scale(dpr, dpr);
    ctx.clearRect(0, 0, w, h);

    const legendEl = $('#pie-legend');
    const groups = {};
    expenses.forEach(r => {
      if (!groups[r.categoryId]) groups[r.categoryId] = { amount: 0, emoji: r.emoji, label: r.label, color: r.color };
      groups[r.categoryId].amount += r.amount;
    });

    const sorted = Object.values(groups).sort((a, b) => b.amount - a.amount);
    const total = sorted.reduce((s, g) => s + g.amount, 0);

    if (total === 0) {
      ctx.fillStyle = 'rgba(255,255,255,0.06)';
      ctx.beginPath();
      ctx.arc(140, 140, 100, 0, Math.PI * 2);
      ctx.fill();
      ctx.fillStyle = '#475569';
      ctx.font = '500 14px Inter, sans-serif';
      ctx.textAlign = 'center';
      ctx.textBaseline = 'middle';
      ctx.fillText('データなし', 140, 140);
      legendEl.innerHTML = '';
      return;
    }

    const cx = 140, cy = 140, radius = 100, innerRadius = 60;
    let startAngle = -Math.PI / 2;

    sorted.forEach(g => {
      const sliceAngle = (g.amount / total) * Math.PI * 2;
      ctx.beginPath();
      ctx.arc(cx, cy, radius, startAngle, startAngle + sliceAngle);
      ctx.arc(cx, cy, innerRadius, startAngle + sliceAngle, startAngle, true);
      ctx.closePath();
      ctx.fillStyle = g.color;
      ctx.fill();

      // Subtle glow
      ctx.shadowColor = g.color;
      ctx.shadowBlur = 6;
      ctx.fill();
      ctx.shadowBlur = 0;

      startAngle += sliceAngle;
    });

    // Center text
    ctx.fillStyle = '#f1f5f9';
    ctx.font = '800 20px Inter, sans-serif';
    ctx.textAlign = 'center';
    ctx.textBaseline = 'middle';
    ctx.fillText(`¥${total.toLocaleString()}`, cx, cy - 6);
    ctx.fillStyle = '#64748b';
    ctx.font = '500 11px Inter, sans-serif';
    ctx.fillText('合計', cx, cy + 14);

    legendEl.innerHTML = sorted.map(g => {
      const pct = ((g.amount / total) * 100).toFixed(1);
      return `
        <div class="legend-item">
          <span class="legend-dot" style="background:${g.color};box-shadow:0 0 6px ${g.color}40"></span>
          <span class="legend-label">${g.emoji} ${g.label}</span>
          <span class="legend-value">¥${g.amount.toLocaleString()} (${pct}%)</span>
        </div>
      `;
    }).join('');
  }

  /* ---- Bar Chart (dark theme) ---- */
  function drawBarChart(expenses, year, month) {
    const canvas = $('#bar-chart');
    const ctx = canvas.getContext('2d');
    const dpr = window.devicePixelRatio || 1;
    const daysInMonth = new Date(year, month + 1, 0).getDate();

    const chartW = Math.max(daysInMonth * 22, canvas.parentElement.clientWidth);
    const chartH = 180;

    canvas.width = chartW * dpr;
    canvas.height = chartH * dpr;
    canvas.style.width = chartW + 'px';
    canvas.style.height = chartH + 'px';
    ctx.scale(dpr, dpr);
    ctx.clearRect(0, 0, chartW, chartH);

    const daily = new Array(daysInMonth).fill(0);
    expenses.forEach(r => {
      const d = new Date(r.date).getDate();
      daily[d - 1] += r.amount;
    });

    const maxVal = Math.max(...daily, 1);
    const barW = 12;
    const gap = (chartW - 40) / daysInMonth;
    const bottom = chartH - 24;
    const topPad = 16;
    const barArea = bottom - topPad;

    // Gridlines
    ctx.strokeStyle = 'rgba(255, 255, 255, 0.04)';
    ctx.lineWidth = 1;
    for (let i = 0; i < 4; i++) {
      const y = topPad + (barArea / 3) * i;
      ctx.beginPath();
      ctx.moveTo(20, y);
      ctx.lineTo(chartW - 10, y);
      ctx.stroke();
    }

    const today = new Date();
    const isCurrentMonth = (today.getFullYear() === year && today.getMonth() === month);

    daily.forEach((val, i) => {
      const x = 24 + i * gap;
      const h = val > 0 ? Math.max((val / maxVal) * barArea, 3) : 0;
      const y = bottom - h;
      const isToday = isCurrentMonth && (i + 1) === today.getDate();

      if (h > 0) {
        const grad = ctx.createLinearGradient(x, y, x, bottom);
        if (isToday) {
          grad.addColorStop(0, '#34d399');
          grad.addColorStop(1, 'rgba(52, 211, 153, 0.15)');
        } else {
          grad.addColorStop(0, '#22d3ee');
          grad.addColorStop(1, 'rgba(34, 211, 238, 0.1)');
        }
        ctx.fillStyle = grad;
        ctx.beginPath();
        ctx.roundRect(x - barW / 2, y, barW, h, [3, 3, 0, 0]);
        ctx.fill();

        // Glow
        ctx.shadowColor = isToday ? '#34d399' : '#22d3ee';
        ctx.shadowBlur = 4;
        ctx.fill();
        ctx.shadowBlur = 0;
      }

      if (daysInMonth <= 15 || (i + 1) % 2 === 1 || (i + 1) === daysInMonth) {
        ctx.fillStyle = isToday ? '#34d399' : '#64748b';
        ctx.font = `${isToday ? '600' : '400'} 9px Inter, sans-serif`;
        ctx.textAlign = 'center';
        ctx.fillText(i + 1, x, chartH - 6);
      }
    });
  }

  /* ---- Score ---- */
  function drawScore(totalExpense) {
    const canvas = $('#score-canvas');
    const ctx = canvas.getContext('2d');
    const dpr = window.devicePixelRatio || 1;
    const size = 160;
    canvas.width = size * dpr;
    canvas.height = size * dpr;
    canvas.style.width = size + 'px';
    canvas.style.height = size + 'px';
    ctx.scale(dpr, dpr);
    ctx.clearRect(0, 0, size, size);

    const cx = size / 2, cy = size / 2, radius = 68, lineW = 8;
    const scoreEl = $('#score-value');
    const msgEl = $('#score-message');

    if (state.budget <= 0) {
      ctx.beginPath();
      ctx.arc(cx, cy, radius, 0, Math.PI * 2);
      ctx.strokeStyle = 'rgba(255,255,255,0.06)';
      ctx.lineWidth = lineW;
      ctx.stroke();
      scoreEl.textContent = '--';
      scoreEl.style.color = '#64748b';
      msgEl.textContent = '予算を設定すると表示されます';
      msgEl.className = 'score-message';
      return;
    }

    const ratio = totalExpense / state.budget;
    let score;
    if (ratio <= 0.5) score = 100;
    else if (ratio <= 0.7) score = 100 - ((ratio - 0.5) / 0.2) * 20;
    else if (ratio <= 1.0) score = 80 - ((ratio - 0.7) / 0.3) * 40;
    else score = Math.max(0, 40 - ((ratio - 1.0) / 0.5) * 40);
    score = Math.round(score);

    let color, msgClass, msg;
    if (score >= 80) { color = '#34d399'; msgClass = 'great'; msg = '🎉 素晴らしい節約っぷり！'; }
    else if (score >= 60) { color = '#22d3ee'; msgClass = 'good'; msg = '👍 いい感じ、この調子！'; }
    else if (score >= 40) { color = '#fb923c'; msgClass = 'warning'; msg = '⚠️ ちょっと使いすぎかも…'; }
    else { color = '#f87171'; msgClass = 'danger'; msg = '🔥 財布がピンチ！'; }

    // BG ring
    ctx.beginPath();
    ctx.arc(cx, cy, radius, 0, Math.PI * 2);
    ctx.strokeStyle = 'rgba(255,255,255,0.06)';
    ctx.lineWidth = lineW;
    ctx.stroke();

    // Score ring
    const startA = -Math.PI / 2;
    const endA = startA + (score / 100) * Math.PI * 2;
    ctx.beginPath();
    ctx.arc(cx, cy, radius, startA, endA);
    ctx.strokeStyle = color;
    ctx.lineWidth = lineW;
    ctx.lineCap = 'round';
    ctx.shadowColor = color;
    ctx.shadowBlur = 10;
    ctx.stroke();
    ctx.shadowBlur = 0;

    scoreEl.textContent = score;
    scoreEl.style.color = color;
    msgEl.textContent = msg;
    msgEl.className = 'score-message ' + msgClass;
  }

  /* ---- History ---- */
  function updateHistory() {
    const d = state.historyMonth;
    const y = d.getFullYear(), m = d.getMonth();
    $('#history-month').textContent = `${y}年${m + 1}月`;

    const records = getMonthRecords(y, m);
    const listEl = $('#history-list');
    const emptyEl = $('#history-empty');

    if (records.length === 0) {
      listEl.innerHTML = '';
      emptyEl.style.display = 'block';
      return;
    }

    emptyEl.style.display = 'none';

    const groups = {};
    records.forEach(r => {
      const dateKey = new Date(r.date).toLocaleDateString('ja-JP', { month: 'long', day: 'numeric', weekday: 'short' });
      if (!groups[dateKey]) groups[dateKey] = [];
      groups[dateKey].push(r);
    });

    listEl.innerHTML = Object.entries(groups).map(([dateLabel, items]) => `
      <div class="history-date-group">
        <div class="history-date-label">${dateLabel}</div>
        ${items.map(r => {
          const sign = r.type === 'expense' ? '-' : '+';
          return `
            <div class="history-item" data-id="${r.id}">
              <span class="history-emoji">${r.emoji}</span>
              <div class="history-info">
                <div class="history-category">${r.label}</div>
                ${r.memo ? `<div class="history-memo-text">${escapeHTML(r.memo)}</div>` : ''}
              </div>
              <div class="history-right">
                <span class="history-amount ${r.type}">${sign}¥${r.amount.toLocaleString()}</span>
                <button class="history-delete-btn" data-id="${r.id}" title="削除">🗑</button>
              </div>
            </div>
          `;
        }).join('')}
      </div>
    `).join('');

    listEl.querySelectorAll('.history-delete-btn').forEach(btn => {
      btn.addEventListener('click', (e) => {
        e.stopPropagation();
        const id = btn.dataset.id;
        showConfirm('この記録を削除しますか？', () => {
          state.records.delete(id);
          deleteRecordFromFirestore(id);
          updateAll();
          showToast('🗑️ 記録を削除しました');
        });
      });
    });
  }

  function navigateHistoryMonth(dir) {
    const d = state.historyMonth;
    state.historyMonth = new Date(d.getFullYear(), d.getMonth() + dir, 1);
    updateHistory();
  }

  /* ---- Settings ---- */
  function openSettings() {
    $('#budget-input').value = state.budget > 0 ? state.budget.toLocaleString() : '';
    if ($('#user-code-text')) $('#user-code-text').textContent = state.userCode || '------';
    $('#settings-modal').classList.add('show');
  }

  function closeSettings() { $('#settings-modal').classList.remove('show'); }

  function saveSettings() {
    const val = parseAmount($('#budget-input').value);
    state.budget = val;
    saveBudget(val);
    updateBudgetBar();
    closeSettings();
    showToast('✅ 設定を保存しました');
  }

  /* ---- Export ---- */
  function exportData() {
    const data = JSON.stringify({ records: getRecordsArray(), budget: state.budget }, null, 2);
    const blob = new Blob([data], { type: 'application/json' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = `zakkuri_kakeibo_${new Date().toISOString().slice(0, 10)}.json`;
    a.click();
    URL.revokeObjectURL(url);
    showToast('📥 データをエクスポートしました');
  }

  /* ---- Update All ---- */
  function updateAll() {
    updateBudgetBar();
    renderRecentItems();
    const active = document.querySelector('.tab-btn.active');
    if (active) {
      if (active.dataset.tab === 'dashboard') updateDashboard();
      if (active.dataset.tab === 'history') updateHistory();
    }
  }

  /* ---- Toast ---- */
  function showToast(message) {
    const toast = $('#toast');
    toast.textContent = message;
    toast.classList.add('show');
    setTimeout(() => toast.classList.remove('show'), 2200);
  }

  /* ---- Confirm ---- */
  let confirmCallback = null;

  function showConfirm(msg, onConfirm) {
    $('#confirm-message').textContent = msg;
    $('#confirm-dialog').classList.add('show');
    confirmCallback = onConfirm;
    const okBtn = $('#confirm-ok');
    const newOk = okBtn.cloneNode(true);
    okBtn.parentNode.replaceChild(newOk, okBtn);
    newOk.addEventListener('click', () => {
      closeConfirm();
      if (confirmCallback) confirmCallback();
    });
  }

  function closeConfirm() {
    $('#confirm-dialog').classList.remove('show');
    confirmCallback = null;
  }

  /* ---- Start ---- */
  document.addEventListener('DOMContentLoaded', init);
})();
