/* ================================================
   ザックリ家計簿 - Application Logic (Firebase版)
   ================================================ */

(function () {
  'use strict';

  // ---- Firebase Config ----
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

  // ---- Categories ----
  const EXPENSE_CATEGORIES = [
    { id: 'food', emoji: '🍔', label: '食費', color: '#fb923c' },
    { id: 'supermarket', emoji: '🛒', label: 'スーパー', color: '#4ade80' },
    { id: 'eating_out', emoji: '🍽️', label: '外食', color: '#f97316' },
    { id: 'drink', emoji: '🍺', label: '飲み代', color: '#facc15' },
    { id: 'housing', emoji: '🏠', label: '家賃', color: '#60a5fa' },
    { id: 'transport', emoji: '🚃', label: '交通費', color: '#34d399' },
    { id: 'entertainment', emoji: '🎮', label: '娯楽', color: '#a855f7' },
    { id: 'clothing', emoji: '👕', label: '衣服', color: '#f472b6' },
    { id: 'medical', emoji: '💊', label: '医療', color: '#f87171' },
    { id: 'telecom', emoji: '📱', label: '通信費', color: '#38bdf8' },
    { id: 'other', emoji: '🔧', label: 'その他', color: '#94a3b8' },
  ];

  const INCOME_CATEGORIES = [
    { id: 'salary', emoji: '💼', label: '給料', color: '#10b981' },
    { id: 'bonus', emoji: '🎁', label: 'ボーナス', color: '#facc15' },
    { id: 'sidejob', emoji: '💻', label: '副業', color: '#60a5fa' },
    { id: 'investment', emoji: '📈', label: '投資', color: '#a855f7' },
    { id: 'refund', emoji: '🔄', label: '返金', color: '#38bdf8' },
    { id: 'other_in', emoji: '💰', label: 'その他', color: '#94a3b8' },
  ];

  // ---- State ----
  let state = {
    records: [],
    budget: 0,
    selectedCategory: null,
    entryType: 'expense',
    dashboardMonth: new Date(),
    historyMonth: new Date(),
    userCode: null,
  };

  let unsubscribeFirestore = null;
  let saveTimeout = null;

  // ---- LocalStorage (for user code only) ----
  const CODE_KEY = 'zakkuri_user_code';

  function getSavedCode() {
    return localStorage.getItem(CODE_KEY);
  }

  function setSavedCode(code) {
    localStorage.setItem(CODE_KEY, code);
  }

  // ---- Generate random code ----
  function generateCode() {
    const chars = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789';
    let code = '';
    for (let i = 0; i < 6; i++) {
      code += chars[Math.floor(Math.random() * chars.length)];
    }
    return code;
  }

  // ---- Firestore read/write ----
  function getUserDocRef() {
    return db.collection('users').doc(state.userCode);
  }

  function saveToFirestore() {
    if (!state.userCode) return;
    clearTimeout(saveTimeout);
    saveTimeout = setTimeout(() => {
      getUserDocRef().set({
        records: state.records,
        budget: state.budget,
        updatedAt: firebase.firestore.FieldValue.serverTimestamp()
      }).catch(e => console.error('Save error:', e));
    }, 500);
  }

  function startFirestoreListener() {
    if (unsubscribeFirestore) unsubscribeFirestore();

    unsubscribeFirestore = getUserDocRef().onSnapshot((doc) => {
      if (doc.exists) {
        const data = doc.data();
        if (data.records) state.records = data.records;
        if (data.budget !== undefined) state.budget = data.budget;
        updateAll();
      }
    }, (err) => {
      console.error('Firestore listener error:', err);
    });
  }

  // ---- DOM Refs ----
  const $ = (sel) => document.querySelector(sel);
  const $$ = (sel) => document.querySelectorAll(sel);

  // ---- Init ----
  function init() {
    const savedCode = getSavedCode();
    if (savedCode) {
      state.userCode = savedCode;
      startApp();
    } else {
      showSetupModal();
    }
  }

  function startApp() {
    renderCategories();
    bindEvents();
    switchTab('input');
    startFirestoreListener();
    updateBudgetBar();
    renderRecentItems();
    if ($('#user-code-text')) {
      $('#user-code-text').textContent = state.userCode;
    }
  }

  // ---- Setup Modal ----
  function showSetupModal() {
    $('#setup-modal').classList.add('show');

    $('#setup-new-btn').addEventListener('click', async () => {
      const code = generateCode();
      state.userCode = code;
      setSavedCode(code);
      // Create empty document in Firestore
      await getUserDocRef().set({
        records: [],
        budget: 0,
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
      // Check if document exists
      const docRef = db.collection('users').doc(code);
      const snap = await docRef.get();
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

    // Allow Enter key
    $('#setup-code-input').addEventListener('keydown', (e) => {
      if (e.key === 'Enter') $('#setup-join-btn').click();
    });
  }

  // ---- Category Rendering ----
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

  // ---- Event Bindings ----
  function bindEvents() {
    // Tab Navigation
    $$('.tab-btn').forEach(btn => {
      btn.addEventListener('click', () => switchTab(btn.dataset.tab));
    });

    // Type Toggle
    $$('.type-btn').forEach(btn => {
      btn.addEventListener('click', () => switchType(btn.dataset.type));
    });

    // Amount Input
    const amountInput = $('#amount-input');
    amountInput.addEventListener('input', () => {
      let val = amountInput.value.replace(/[^\d]/g, '');
      if (val.length > 10) val = val.slice(0, 10);
      amountInput.value = val ? Number(val).toLocaleString() : '';
      validateForm();
    });

    // Quick Amount Buttons
    $$('.quick-btn').forEach(btn => {
      btn.addEventListener('click', () => {
        const current = parseAmount(amountInput.value) || 0;
        const add = Number(btn.dataset.amount);
        amountInput.value = (current + add).toLocaleString();
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
    $$('.preset-btn').forEach(btn => {
      btn.addEventListener('click', () => {
        const input = $('#budget-input');
        input.value = '';
        input.value = Number(btn.dataset.budget).toLocaleString();
        input.focus();
      });
    });

    // Inline budget editing
    $('#budget-total-display').addEventListener('click', startInlineBudgetEdit);
    const inlineInput = $('#budget-inline-input');
    inlineInput.addEventListener('keydown', (e) => {
      if (e.key === 'Enter') finishInlineBudgetEdit();
      if (e.key === 'Escape') cancelInlineBudgetEdit();
    });
    inlineInput.addEventListener('blur', finishInlineBudgetEdit);
    inlineInput.addEventListener('input', () => {
      let val = inlineInput.value.replace(/[^\d]/g, '');
      if (val.length > 10) val = val.slice(0, 10);
      inlineInput.value = val ? Number(val).toLocaleString() : '';
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
      showConfirm('本当に全てのデータを削除しますか？\nこの操作は元に戻せません。', () => {
        state.records = [];
        state.budget = 0;
        saveToFirestore();
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

    // Confirm dialog
    $('#confirm-cancel').addEventListener('click', closeConfirm);
  }

  // ---- Tab Switching ----
  function switchTab(tab) {
    $$('.tab-btn').forEach(b => b.classList.toggle('active', b.dataset.tab === tab));
    $$('.tab-content').forEach(c => c.classList.remove('active'));
    $(`#tab-${tab}`).classList.add('active');

    if (tab === 'dashboard') updateDashboard();
    if (tab === 'history') updateHistory();
  }

  // ---- Type Toggle ----
  function switchType(type) {
    state.entryType = type;
    state.selectedCategory = null;
    $$('.type-btn').forEach(b => b.classList.toggle('active', b.dataset.type === type));
    renderCategories();
    validateForm();
  }

  // ---- Form Validation ----
  function validateForm() {
    const amount = parseAmount($('#amount-input').value);
    const valid = state.selectedCategory && amount > 0;
    $('#save-btn').disabled = !valid;
  }

  function parseAmount(str) {
    return Number(String(str).replace(/[^\d]/g, '')) || 0;
  }

  // ---- Save Record ----
  function saveRecord() {
    const amount = parseAmount($('#amount-input').value);
    if (!amount || !state.selectedCategory) return;

    const cats = getCategories();
    const cat = cats.find(c => c.id === state.selectedCategory);

    const record = {
      id: Date.now().toString(36) + Math.random().toString(36).slice(2, 6),
      type: state.entryType,
      categoryId: cat.id,
      emoji: cat.emoji,
      label: cat.label,
      color: cat.color,
      amount: amount,
      memo: $('#memo-input').value.trim(),
      date: new Date().toISOString(),
    };

    state.records.unshift(record);
    saveToFirestore();

    // Reset form
    $('#amount-input').value = '';
    $('#memo-input').value = '';
    state.selectedCategory = null;
    renderCategories();
    validateForm();

    const emoji = state.entryType === 'expense' ? '💸' : '💰';
    showToast(`${emoji} ¥${amount.toLocaleString()} を記録しました`);

    updateBudgetBar();
    renderRecentItems();

    const btn = $('#save-btn');
    btn.style.transform = 'scale(0.95)';
    setTimeout(() => { btn.style.transform = ''; }, 150);
  }

  // ---- Recent Items ----
  function renderRecentItems() {
    const list = $('#recent-list');
    const recent = state.records.slice(0, 5);

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

  // ---- Budget Bar ----
  function updateBudgetBar() {
    const now = new Date();
    const monthRecords = getMonthRecords(now.getFullYear(), now.getMonth());
    const spent = monthRecords
      .filter(r => r.type === 'expense')
      .reduce((s, r) => s + r.amount, 0);

    const remaining = state.budget > 0 ? state.budget - spent : 0;
    const pct = state.budget > 0 ? Math.min((spent / state.budget) * 100, 100) : 0;

    const numeratorEl = $('#budget-spent-amount');
    const denominatorEl = $('#budget-total-display');
    const progressEl = $('#budget-progress');
    const remainLabel = $('#budget-remaining-label');
    const pctLabel = $('#budget-pct-label');

    numeratorEl.textContent = `¥${spent.toLocaleString()}`;

    if (state.budget <= 0) {
      denominatorEl.textContent = '未設定 ✎';
      progressEl.style.width = '0%';
      progressEl.className = 'progress-fill';
      numeratorEl.className = 'budget-numerator';
      remainLabel.textContent = '';
      pctLabel.textContent = 'タップして予算を設定';
    } else {
      denominatorEl.textContent = `¥${state.budget.toLocaleString()}`;

      if (pct >= 90) {
        numeratorEl.className = 'budget-numerator danger';
        progressEl.className = 'progress-fill danger';
      } else if (pct >= 70) {
        numeratorEl.className = 'budget-numerator warning';
        progressEl.className = 'progress-fill warning';
      } else {
        numeratorEl.className = 'budget-numerator';
        progressEl.className = 'progress-fill';
      }

      progressEl.style.width = pct + '%';
      remainLabel.textContent = `残り ¥${Math.max(remaining, 0).toLocaleString()}`;
      pctLabel.textContent = `${pct.toFixed(0)}% 消化`;
    }
  }

  // ---- Inline Budget Edit ----
  function startInlineBudgetEdit() {
    const displayEl = $('#budget-total-display');
    const editEl = $('#budget-total-edit');
    const input = $('#budget-inline-input');

    displayEl.style.display = 'none';
    editEl.style.display = 'flex';
    input.value = state.budget > 0 ? state.budget.toLocaleString() : '';
    input.focus();
    input.select();
  }

  function finishInlineBudgetEdit() {
    const displayEl = $('#budget-total-display');
    const editEl = $('#budget-total-edit');
    const input = $('#budget-inline-input');

    if (editEl.style.display === 'none') return;

    const val = parseAmount(input.value);
    state.budget = val;
    saveToFirestore();

    displayEl.style.display = '';
    editEl.style.display = 'none';

    updateBudgetBar();
    if (val > 0) showToast(`✅ 予算を ¥${val.toLocaleString()} に設定しました`);
  }

  function cancelInlineBudgetEdit() {
    const displayEl = $('#budget-total-display');
    const editEl = $('#budget-total-edit');
    displayEl.style.display = '';
    editEl.style.display = 'none';
  }

  // ---- Dashboard ----
  function updateDashboard() {
    const d = state.dashboardMonth;
    const y = d.getFullYear();
    const m = d.getMonth();

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

  // ---- Pie Chart ----
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
      if (!groups[r.categoryId]) {
        groups[r.categoryId] = { amount: 0, emoji: r.emoji, label: r.label, color: r.color };
      }
      groups[r.categoryId].amount += r.amount;
    });

    const sorted = Object.values(groups).sort((a, b) => b.amount - a.amount);
    const total = sorted.reduce((s, g) => s + g.amount, 0);

    if (total === 0) {
      ctx.fillStyle = '#e5e7eb';
      ctx.beginPath();
      ctx.arc(140, 140, 100, 0, Math.PI * 2);
      ctx.fill();
      ctx.fillStyle = '#9ca3af';
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
      startAngle += sliceAngle;
    });

    ctx.fillStyle = '#1a1d23';
    ctx.font = '800 20px Inter, sans-serif';
    ctx.textAlign = 'center';
    ctx.textBaseline = 'middle';
    ctx.fillText(`¥${total.toLocaleString()}`, cx, cy - 6);
    ctx.fillStyle = '#6b7280';
    ctx.font = '500 11px Inter, sans-serif';
    ctx.fillText('合計', cx, cy + 14);

    legendEl.innerHTML = sorted.map(g => {
      const pct = ((g.amount / total) * 100).toFixed(1);
      return `
        <div class="legend-item">
          <span class="legend-dot" style="background:${g.color}"></span>
          <span class="legend-label">${g.emoji} ${g.label}</span>
          <span class="legend-value">¥${g.amount.toLocaleString()} (${pct}%)</span>
        </div>
      `;
    }).join('');
  }

  // ---- Bar Chart ----
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

    ctx.strokeStyle = 'rgba(0, 0, 0, 0.06)';
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
          grad.addColorStop(0, '#10b981');
          grad.addColorStop(1, 'rgba(16, 185, 129, 0.2)');
        } else {
          grad.addColorStop(0, '#0891b2');
          grad.addColorStop(1, 'rgba(8, 145, 178, 0.15)');
        }
        ctx.fillStyle = grad;
        ctx.beginPath();
        ctx.roundRect(x - barW / 2, y, barW, h, [3, 3, 0, 0]);
        ctx.fill();
      }

      if (daysInMonth <= 15 || (i + 1) % 2 === 1 || (i + 1) === daysInMonth) {
        ctx.fillStyle = isToday ? '#10b981' : '#9ca3af';
        ctx.font = `${isToday ? '600' : '400'} 9px Inter, sans-serif`;
        ctx.textAlign = 'center';
        ctx.fillText(i + 1, x, chartH - 6);
      }
    });
  }

  // ---- Score ----
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
      ctx.strokeStyle = '#e5e7eb';
      ctx.lineWidth = lineW;
      ctx.stroke();
      scoreEl.textContent = '--';
      scoreEl.style.color = '#9ca3af';
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
    if (score >= 80) { color = '#10b981'; msgClass = 'great'; msg = '🎉 素晴らしい節約っぷり！'; }
    else if (score >= 60) { color = '#0891b2'; msgClass = 'good'; msg = '👍 いい感じ、この調子！'; }
    else if (score >= 40) { color = '#f97316'; msgClass = 'warning'; msg = '⚠️ ちょっと使いすぎかも…'; }
    else { color = '#ef4444'; msgClass = 'danger'; msg = '🔥 財布がピンチ！'; }

    ctx.beginPath();
    ctx.arc(cx, cy, radius, 0, Math.PI * 2);
    ctx.strokeStyle = '#e5e7eb';
    ctx.lineWidth = lineW;
    ctx.stroke();

    const startA = -Math.PI / 2;
    const endA = startA + (score / 100) * Math.PI * 2;
    ctx.beginPath();
    ctx.arc(cx, cy, radius, startA, endA);
    ctx.strokeStyle = color;
    ctx.lineWidth = lineW;
    ctx.lineCap = 'round';
    ctx.stroke();

    scoreEl.textContent = score;
    scoreEl.style.color = color;
    msgEl.textContent = msg;
    msgEl.className = 'score-message ' + msgClass;
  }

  // ---- History ----
  function updateHistory() {
    const d = state.historyMonth;
    const y = d.getFullYear();
    const m = d.getMonth();

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
          state.records = state.records.filter(r => r.id !== id);
          saveToFirestore();
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

  // ---- Settings ----
  function openSettings() {
    $('#budget-input').value = state.budget > 0 ? state.budget.toLocaleString() : '';
    if ($('#user-code-text')) {
      $('#user-code-text').textContent = state.userCode || '------';
    }
    $('#settings-modal').classList.add('show');
  }

  function closeSettings() {
    $('#settings-modal').classList.remove('show');
  }

  function saveSettings() {
    const val = parseAmount($('#budget-input').value);
    state.budget = val;
    saveToFirestore();
    updateBudgetBar();
    closeSettings();
    showToast('✅ 設定を保存しました');
  }

  // ---- Export ----
  function exportData() {
    const data = JSON.stringify({ records: state.records, budget: state.budget }, null, 2);
    const blob = new Blob([data], { type: 'application/json' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = `zakkuri_kakeibo_${new Date().toISOString().slice(0, 10)}.json`;
    a.click();
    URL.revokeObjectURL(url);
    showToast('📥 データをエクスポートしました');
  }

  // ---- Utilities ----
  function getMonthRecords(year, month) {
    return state.records.filter(r => {
      const d = new Date(r.date);
      return d.getFullYear() === year && d.getMonth() === month;
    }).sort((a, b) => new Date(b.date) - new Date(a.date));
  }

  function updateAll() {
    updateBudgetBar();
    renderRecentItems();
    const activeTab = document.querySelector('.tab-btn.active');
    if (activeTab) {
      if (activeTab.dataset.tab === 'dashboard') updateDashboard();
      if (activeTab.dataset.tab === 'history') updateHistory();
    }
  }

  function escapeHTML(str) {
    const div = document.createElement('div');
    div.textContent = str;
    return div.innerHTML;
  }

  // ---- Toast ----
  function showToast(message) {
    const toast = $('#toast');
    toast.textContent = message;
    toast.classList.add('show');
    setTimeout(() => toast.classList.remove('show'), 2200);
  }

  // ---- Confirm Dialog ----
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

  // ---- Start ----
  document.addEventListener('DOMContentLoaded', init);
})();
