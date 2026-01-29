/**
 * SLG竞对数据监测演示
 * 公司维度 - 大盘数据：data/{year}/{week}_formatted.json（步骤一产出）
 * 产品维度 - 爆量产品地区数据：data/{year}/{week}/product_strategy_old|new.json（final_join 表转 JSON）
 * 素材维度 - advertisements/{year}/{week}/{productType}/{folder}/json 下创意 JSON，网页直接播放视频
 */
(function () {
  const base = document.location.pathname.startsWith('/frontend') ? '/frontend' : '';
  const adsBase = ''; // 相对站点根路径，如 /advertisements

  function creativeVideoSrc(url) {
    // 直接返回原始 URL，让客户端用自己的网络播放，不经过服务器代理
    return url || '';
  }

  const localTimeEl = document.getElementById('localTime');
  const sidebarYearsEl = document.getElementById('sidebarYears');
  const mainTitleEl = document.getElementById('mainTitle');
  const panelCompany = document.getElementById('panelCompany');
  const panelProduct = document.getElementById('panelProduct');
  const panelCreative = document.getElementById('panelCreative');
  const panelCombo = document.getElementById('panelCombo');
  const comboTitleEl = document.getElementById('comboTitle');
  const comboPeriodEl = document.getElementById('comboPeriod');
  const tableWrap = document.getElementById('tableWrap');
  const productTitleEl = document.getElementById('productTitle');
  const productPeriodEl = document.getElementById('productPeriod');
  const productSearchInput = document.getElementById('productSearchInput');
  const productSelect = document.getElementById('productSelect');
  const creativeTitleEl = document.getElementById('creativeTitle');
  const creativePeriodHintEl = document.getElementById('creativePeriodHint');
  const creativeProductTable = document.getElementById('creativeProductTable');
  const creativeProductSelect = document.getElementById('creativeProductSelect');
  const creativeRegionFilter = document.getElementById('creativeRegionFilter');
  const creativeDurationFilter = document.getElementById('creativeDurationFilter');
  const creativeRatioFilter = document.getElementById('creativeRatioFilter');
  const creativeSearchInput = document.getElementById('creativeSearchInput');
  const creativeProductDataRow = document.getElementById('creativeProductDataRow');
  const creativeProductDataHead = document.getElementById('creativeProductDataHead');
  const creativeProductDataBody = document.getElementById('creativeProductDataBody');
  const btnDownload = document.getElementById('btnDownload');
  const btnProductDownload = document.getElementById('btnProductDownload');
  const btnCreativeDownload = document.getElementById('btnCreativeDownload');
  const tableHead = document.getElementById('tableHead');
  const tableBody = document.getElementById('tableBody');
  const searchInput = document.getElementById('searchInput');
  const loadingEl = document.getElementById('loading');
  const emptyEl = document.getElementById('empty');
  const errorEl = document.getElementById('error');

  let weeksIndex = null;
  let currentYear = null;
  let currentWeek = null;
  let currentDimension = 'company';
  let currentCompanySubTab = 'overall';
  (function () {
    var h = (window.location.hash || '#company').replace('#', '').toLowerCase();
    if (['company', 'product', 'creative', 'combo'].indexOf(h) >= 0) currentDimension = h;
  })();
  let currentData = null;
  let filteredRows = null;
  let sortCol = -1;
  let sortAsc = true;
  let creativeProductsIndex = null;
  let creativeRows = null;
  let creativeFilteredRows = null;
  let productDataForCreative = null;
  let pendingCreativeProduct = null;  // 从公司维度点击标黄行跳转素材维度时待选中的产品归属
  const CREATIVE_REGIONS = [{ key: '亚洲T1', label: '亚洲 T1 市场' }, { key: '欧美T1', label: '欧美 T1 市场' }, { key: 'T2', label: 'T2 市场' }, { key: 'T3', label: 'T3 市场' }];
  const YELLOW_BG = '#fff2cc';
  const CREATIVE_TAG_OPTIONS = ['数字门跑酷', '塔防', '肉鸽/幸存者 like/割草'];

  function normalizeProductName(s) {
    if (!s || typeof s !== 'string') return '';
    return s.replace(/[:\-_]\s*/g, ' ').replace(/\s+/g, ' ').trim().toLowerCase();
  }

  function productNamesMatch(pending, name, disp) {
    if (!pending) return false;
    var p = normalizeProductName(pending);
    var n = normalizeProductName(name || '');
    var d = normalizeProductName(disp || '');
    if (!p) return false;
    return p.indexOf(n) >= 0 || p.indexOf(d) >= 0 || n.indexOf(p) >= 0 || d.indexOf(p) >= 0 ||
      (n && p === n) || (d && p === d);
  }

  const MONTH_NAMES = ['01月', '02月', '03月', '04月', '05月', '06月', '07月', '08月', '09月', '10月', '11月', '12月'];

  function show(el) { el.style.display = ''; }
  function hide(el) { el.style.display = 'none'; }

  function setState(state) {
    hide(loadingEl);
    hide(emptyEl);
    hide(errorEl);
    tableHead.innerHTML = '';
    tableBody.innerHTML = '';
    if (state === 'loading') show(loadingEl);
    else if (state === 'empty') show(emptyEl);
    else if (state === 'error') show(errorEl);
  }

  function updateLocalTime() {
    const now = new Date();
    const s = now.getFullYear() + '/' +
      String(now.getMonth() + 1).padStart(2, '0') + '/' +
      String(now.getDate()).padStart(2, '0') + ' ' +
      String(now.getHours()).padStart(2, '0') + ':' +
      String(now.getMinutes()).padStart(2, '0') + ':' +
      String(now.getSeconds()).padStart(2, '0');
    localTimeEl.textContent = '本机时间: ' + s;
  }

  function applyCellStyle(td, style) {
    if (!style) return;
    if (style.bg_color && !String(style.bg_color).startsWith('#Values')) td.style.backgroundColor = style.bg_color;
    if (style.font_color && String(style.font_color).startsWith('#')) td.style.color = style.font_color;
    if (style.bold) td.style.fontWeight = 'bold';
  }

  function isRedFont(style) {
    if (!style || !style.font_color) return false;
    var c = String(style.font_color).toLowerCase().replace('#', '');
    return c === 'ff0000';
  }

  function isTargetProductRow(styleRow) {
    if (!styleRow || !Array.isArray(styleRow)) return false;
    return styleRow.some(function (s) {
      if (!s || !s.bg_color) return false;
      var bg = String(s.bg_color).toLowerCase().replace('#', '');
      return bg === 'fff2cc' || bg === 'c5dbf7';
    });
  }

  function cellClass(cellText) {
    const s = String(cellText ?? '');
    if (s.includes('▲')) return 'cell-up';
    if (s.includes('▼')) return 'cell-down';
    return '';
  }

  function isSummaryRow(row) {
    const first = String(row[0] ?? '');
    return first.includes('汇总');
  }

  /** 统一表格单元格显示：流水$+千分位无小数，变动百分比+箭头，日期 xxxx/xx/xx，数字千分位 */
  function formatCell(headerName, val) {
    if (val == null || val === '') return '';
    const s = String(val).trim();
    const n = Number(val);
    const isNum = !Number.isNaN(n);
    const h = (headerName || '').trim();

    if (h.includes('流水') && isNum) {
      const intVal = Math.round(n);
      return '$' + intVal.toLocaleString('en-US');
    }
    if (h.includes('变动')) {
      if (s.includes('%') && (s.includes('▲') || s.includes('▼'))) return s;
      if (isNum) {
        const pct = (n * 100).toFixed(2);
        const arrow = n >= 0 ? '▲' : '▼';
        const sign = n >= 0 ? '+' : '';
        return sign + pct + '%' + arrow;
      }
      return s;
    }
    if (h.includes('时间') || h.includes('上线') || h === '首次看到' || /^\d{4}[-/]\d{1,2}[-/]\d{1,2}/.test(s)) {
      const match = s.match(/(\d{4})[-/](\d{1,2})[-/](\d{1,2})/);
      if (match) return match[1] + '/' + String(match[2]).padStart(2, '0') + '/' + String(match[3]).padStart(2, '0');
      return s;
    }
    if (isNum) {
      if (Number.isInteger(n)) return n.toLocaleString('en-US');
      if (Math.abs(n) >= 1 || n === 0) return n.toLocaleString('en-US', { minimumFractionDigits: 0, maximumFractionDigits: 2 });
      return n.toLocaleString('en-US', { minimumFractionDigits: 0, maximumFractionDigits: 4 });
    }
    return s;
  }

  function formatNum(val) {
    if (val == null || val === '') return '';
    const n = Number(val);
    if (Number.isNaN(n)) return String(val);
    return n.toLocaleString('en-US');
  }

  function renderTable(isProductDimension) {
    if (!currentData || !currentData.headers || !currentData.rows) return;
    const headers = currentData.headers;
    const styles = currentData.styles || [];
    filteredRows = filteredRows || currentData.rows.slice();
    const productColIndex = headers.indexOf('产品归属');

    tableHead.innerHTML = '';
    const theadTr = document.createElement('tr');
    headers.forEach((h, i) => {
      const th = document.createElement('th');
      th.textContent = h;
      th.dataset.col = i;
      th.dataset.sort = '';
      if (styles[0] && styles[0][i]) applyCellStyle(th, styles[0][i]);
      th.addEventListener('click', () => sortBy(i));
      theadTr.appendChild(th);
    });
    tableHead.appendChild(theadTr);

    tableBody.innerHTML = '';
    filteredRows.forEach((row, rowIdx) => {
      const tr = document.createElement('tr');
      if (isSummaryRow(row)) tr.classList.add('row-summary');
      const styleRow = styles[currentData.rows.indexOf(row) + 1] || styles[rowIdx + 1];
      if (!isProductDimension && !isSummaryRow(row) && isTargetProductRow(styleRow) && productColIndex >= 0 && row[productColIndex] != null) {
        tr.classList.add('target-product-row');
        tr.dataset.product = String(row[productColIndex]);
      }
      row.forEach((cell, colIdx) => {
        const td = document.createElement('td');
        const isProductNameLink = (isProductDimension || (!isSummaryRow(row) && currentDimension === 'company' && isTargetProductRow(styleRow))) && colIdx === productColIndex && productColIndex >= 0;
        if (isProductNameLink && cell != null && String(cell) !== '') {
          const a = document.createElement('a');
          a.href = '#';
          a.className = 'product-link';
          a.textContent = String(cell);
          td.appendChild(a);
        } else {
          td.textContent = formatCell(headers[colIdx], cell);
        }
        const cls = cellClass(cell);
        if (cls) td.classList.add(cls);
        if (styleRow && styleRow[colIdx]) applyCellStyle(td, styleRow[colIdx]);
        if (colIdx === productColIndex && styleRow && styleRow[colIdx] && isRedFont(styleRow[colIdx])) td.style.textDecoration = 'line-through';  // 仅产品归属列标红画删除线
        if (typeof cell === 'number' || (typeof cell === 'string' && /^[\d.-]+$/.test(cell))) td.classList.add('num');
        tr.appendChild(td);
      });
      tableBody.appendChild(tr);
    });

    hide(loadingEl);
    hide(emptyEl);
    hide(errorEl);
  }

  function sortBy(colIndex) {
    if (!currentData || !currentData.rows.length) return;
    if (sortCol === colIndex) sortAsc = !sortAsc;
    else { sortCol = colIndex; sortAsc = true; }

    filteredRows.sort((a, b) => {
      const av = a[colIndex];
      const bv = b[colIndex];
      const anum = Number(av);
      const bnum = Number(bv);
      if (!Number.isNaN(anum) && !Number.isNaN(bnum)) return sortAsc ? anum - bnum : bnum - anum;
      return sortAsc ? String(av ?? '').localeCompare(bv ?? '') : String(bv ?? '').localeCompare(av ?? '');
    });

    document.querySelectorAll('#tableHead th').forEach((th, i) => {
      th.dataset.sort = i === colIndex ? (sortAsc ? 'asc' : 'desc') : '';
    });
    tableBody.innerHTML = '';
    const styles = currentData.styles || [];
    const productColIndex = currentData.headers.indexOf('产品归属');
    filteredRows.forEach((row, idx) => {
      const tr = document.createElement('tr');
      if (isSummaryRow(row)) tr.classList.add('row-summary');
      const origIdx = currentData.rows.indexOf(row);
      const styleRow = origIdx >= 0 ? styles[origIdx + 1] : styles[idx + 1];
      if (currentDimension === 'company' && !isSummaryRow(row) && isTargetProductRow(styleRow) && productColIndex >= 0 && row[productColIndex] != null) {
        tr.classList.add('target-product-row');
        tr.dataset.product = String(row[productColIndex]);
      }
      const isProductDim = currentDimension === 'product';
      row.forEach((cell, colIdx) => {
        const td = document.createElement('td');
        const isProductNameLink = (isProductDim || (!isSummaryRow(row) && currentDimension === 'company' && isTargetProductRow(styleRow))) && colIdx === productColIndex && productColIndex >= 0;
        if (isProductNameLink && cell != null && String(cell) !== '') {
          const a = document.createElement('a');
          a.href = '#';
          a.className = 'product-link';
          a.textContent = String(cell);
          td.appendChild(a);
        } else {
          td.textContent = formatCell(currentData.headers[colIdx], cell);
        }
        const cls = cellClass(cell);
        if (cls) td.classList.add(cls);
        if (styleRow && styleRow[colIdx]) applyCellStyle(td, styleRow[colIdx]);
        if (currentDimension === 'company' && colIdx === productColIndex && styleRow && styleRow[colIdx] && isRedFont(styleRow[colIdx])) td.style.textDecoration = 'line-through';
        if (typeof cell === 'number' || (typeof cell === 'string' && /^[\d.-]+$/.test(cell))) td.classList.add('num');
        tr.appendChild(td);
      });
      tableBody.appendChild(tr);
    });
  }

  function filterRows() {
    const input = currentDimension === 'product' ? productSearchInput : searchInput;
    const q = (input.value || '').trim().toLowerCase();
    if (!currentData || !currentData.rows) return;
    if (!q) filteredRows = currentData.rows.slice();
    else {
      filteredRows = currentData.rows.filter(row =>
        row.some(cell => String(cell ?? '').toLowerCase().includes(q))
      );
    }
    sortCol = -1;
    renderTable(currentDimension === 'product');
  }

  function loadCompanyWeek(year, week) {
    if (!year || !week) {
      mainTitleEl.textContent = '请从左侧选择周期';
      setState('empty');
      return;
    }
    mainTitleEl.textContent = year + '年, ' + week + ', SLG竞对数据监测表 (大盘数据)';
    setState('loading');
    const url = base + '/data/' + year + '/' + week + '_formatted.json';
    fetch(url)
      .then(r => { if (!r.ok) throw new Error(r.statusText || '加载失败'); return r.json(); })
      .then(data => {
        currentData = { headers: data.headers || [], rows: data.rows || [], styles: data.styles || [] };
        filteredRows = currentData.rows.slice();
        sortCol = -1;
        setActiveWeekInSidebar(year, week);
        if (currentCompanySubTab === 'overall') {
          renderTable(false);
          hide(loadingEl);
          hide(emptyEl);
          hide(errorEl);
        } else {
          hide(tableWrap);
          hide(loadingEl);
          hide(errorEl);
          show(emptyEl);
          emptyEl.textContent = '功能开发中';
        }
      })
      .catch(err => { errorEl.textContent = '加载失败: ' + err.message; setState('error'); });
  }

  function showCompanySubTabContent() {
    if (currentCompanySubTab !== 'overall') {
      hide(tableWrap);
      hide(loadingEl);
      hide(errorEl);
      show(emptyEl);
      emptyEl.textContent = '功能开发中';
    } else {
      show(tableWrap);
      hide(emptyEl);
      if (currentData && currentDimension === 'company') renderTable(false);
    }
  }

  function loadProductWeek(year, week) {
    if (!year || !week) {
      productTitleEl.textContent = '请从左侧选择周期';
      productPeriodEl.textContent = '--';
      setState('empty');
      return;
    }
    productTitleEl.textContent = year + '年, ' + week + ', 爆量产品地区数据';
    productPeriodEl.textContent = week;
    const key = productSelect.value || 'old';
    setState('loading');
    const url = base + '/data/' + year + '/' + week + '/product_strategy_' + key + '.json';
    fetch(url)
      .then(r => { if (!r.ok) throw new Error(r.statusText || '加载失败'); return r.json(); })
      .then(data => {
        currentData = { headers: data.headers || [], rows: data.rows || [], styles: [] };
        filteredRows = currentData.rows.slice();
        sortCol = -1;
        renderTable(true);
        setActiveWeekInSidebar(year, week);
      })
      .catch(err => { errorEl.textContent = '加载失败: ' + err.message + '（请先运行 convert_final_join_to_json.py 生成 JSON）'; setState('error'); });
  }

  function loadWeek(year, week) {
    currentYear = year;
    currentWeek = week;
    if (currentDimension === 'company') loadCompanyWeek(year, week);
    else if (currentDimension === 'product') loadProductWeek(year, week);
    else if (currentDimension === 'creative') loadCreativeWeek(year, week);
    else if (currentDimension === 'combo') updateComboDisplay();
  }

  function loadCreativeProductsThen(year, week) {
    creativeTitleEl.textContent = year + '年, ' + week + ', 素材维度';
    if (creativePeriodHintEl) {
      creativePeriodHintEl.textContent = '当前周期与公司维度、产品维度一致：' + year + '年 ' + week;
      creativePeriodHintEl.style.display = '';
    }
    const url = base + '/data/' + year + '/' + week + '/creative_products.json';
    fetch(url)
      .then(r => { if (!r.ok) throw new Error(r.statusText); return r.json(); })
      .then(index => {
        creativeProductsIndex = index;
        var fallbacks = [];
        if (!index.strategy_old || index.strategy_old.length === 0) {
          fallbacks.push(fetch(base + '/data/' + year + '/' + week + '/product_strategy_old.json').then(r => r.ok ? r.json() : {}).then(data => {
            var headers = data.headers || [];
            var rows = data.rows || [];
            var col = headers.indexOf('产品归属');
            if (col >= 0 && rows.length) {
              index.strategy_old = rows.map(function (row) { return { product_name: String(row[col] || ''), display: String(row[col] || ''), folder: '', app_id: '', noCreative: true }; });
            }
          }));
        }
        if (!index.strategy_new || index.strategy_new.length === 0) {
          fallbacks.push(fetch(base + '/data/' + year + '/' + week + '/product_strategy_new.json').then(r => r.ok ? r.json() : {}).then(data => {
            var headers = data.headers || [];
            var rows = data.rows || [];
            var col = headers.indexOf('产品归属');
            if (col >= 0 && rows.length) {
              index.strategy_new = rows.map(function (row) { return { product_name: String(row[col] || ''), display: String(row[col] || ''), folder: '', app_id: '', noCreative: true }; });
            }
          }));
        }
        return Promise.all(fallbacks).then(function () { return index; });
      })
      .then(index => {
        fillCreativeProductSelect();
        creativeProductDataRow.style.display = 'none';
        creativeRows = null;
        creativeFilteredRows = null;
        if (pendingCreativeProduct) {
          var ptype = creativeProductTable.value || 'strategy_old';
          var list = (index[ptype] || []);
          var foundIdx = -1;
          for (var i = 0; i < list.length; i++) {
            var p = list[i];
            if (productNamesMatch(pendingCreativeProduct, p.product_name, p.display)) {
              foundIdx = i;
              break;
            }
          }
          if (foundIdx < 0) {
            var otherType = ptype === 'strategy_old' ? 'strategy_new' : 'strategy_old';
            var otherList = index[otherType] || [];
            for (var j = 0; j < otherList.length; j++) {
              var q = otherList[j];
              if (productNamesMatch(pendingCreativeProduct, q.product_name, q.display)) {
                creativeProductTable.value = otherType;
                fillCreativeProductSelect();
                creativeProductSelect.value = String(j);
                foundIdx = j;
                list = otherList;
                break;
              }
            }
          }
          if (foundIdx >= 0) {
            creativeProductSelect.value = String(foundIdx);
            loadCreativeProductAds();
          } else {
            hide(loadingEl); hide(errorEl); renderCreativeTableIfNeeded();
          }
          pendingCreativeProduct = null;
        } else if (creativeProductSelect.value) {
          loadCreativeProductAds();
        } else {
          hide(loadingEl); hide(errorEl); renderCreativeTableIfNeeded();
        }
      })
      .catch(() => {
        creativeProductsIndex = null;
        creativeProductSelect.innerHTML = '<option value="">-- 加载失败，请确认本周期已生成 creative_products.json --</option>';
        creativeProductDataRow.style.display = 'none';
        setState('error');
        errorEl.textContent = '加载素材产品索引失败';
      });
  }

  function fillCreativeProductSelect() {
    const ptype = creativeProductTable.value || 'strategy_old';
    const list = (creativeProductsIndex && creativeProductsIndex[ptype]) ? creativeProductsIndex[ptype] : [];
    const oldList = (creativeProductsIndex && creativeProductsIndex.strategy_old) ? creativeProductsIndex.strategy_old : [];
    const newList = (creativeProductsIndex && creativeProductsIndex.strategy_new) ? creativeProductsIndex.strategy_new : [];
    const isEmpty = oldList.length === 0 && newList.length === 0;
    creativeProductSelect.innerHTML = '';
    if (isEmpty) {
      const opt = document.createElement('option');
      opt.value = '';
      opt.textContent = '-- 本周期暂无素材数据（与公司/产品维度周期一致） --';
      creativeProductSelect.appendChild(opt);
    } else {
      creativeProductSelect.innerHTML = '<option value="">-- 请选择产品 --</option>';
      list.forEach((p, i) => {
        const opt = document.createElement('option');
        opt.value = i;
        opt.textContent = p.display || p.product_name;
        opt.dataset.folder = p.folder;
        opt.dataset.appId = p.app_id;
        opt.dataset.productName = p.product_name;
        creativeProductSelect.appendChild(opt);
      });
    }
    creativeProductSelect.value = '';
  }

  function loadCreativeWeek(year, week) {
    if (!year || !week) {
      creativeTitleEl.textContent = '请从左侧选择周期';
      if (creativePeriodHintEl) creativePeriodHintEl.style.display = 'none';
      creativeProductSelect.innerHTML = '<option value="">-- 请先选择周期 --</option>';
      creativeProductDataRow.style.display = 'none';
      setState('empty');
      return;
    }
    setState('loading');
    loadCreativeProductsThen(year, week);
    setActiveWeekInSidebar(year, week);
  }

  function getAdsJsonPath(folder, appId, region) {
    const enc = encodeURIComponent(folder);
    return (adsBase || '') + '/advertisements/' + currentYear + '/' + currentWeek + '/' + creativeProductTable.value + '/' + enc + '/json/' + appId + '_' + region + '.json';
  }

  function loadCreativeProductAds() {
    const idx = parseInt(creativeProductSelect.value, 10);
    const ptype = creativeProductTable.value || 'strategy_old';
    const list = (creativeProductsIndex && creativeProductsIndex[ptype]) ? creativeProductsIndex[ptype] : [];
    const p = list[idx];
    if (!p) { creativeRows = null; creativeFilteredRows = null; renderCreativeTableIfNeeded(); return; }
    if (p.noCreative || !p.folder || !p.app_id) {
      creativeRows = [];
      creativeFilteredRows = [];
      creativeProductDataRow.style.display = 'none';
      productDataForCreative = null;
      hide(loadingEl); hide(errorEl); show(emptyEl);
      emptyEl.textContent = '本周期该产品表暂无素材数据（可先执行流水线第二步拉取创意数据，或从左侧切换至有数据的周期如 2026-0119-0125 查看）';
      renderCreativeTableIfNeeded();
      return;
    }
    setState('loading');
    const regionKeys = ['亚洲T1', '欧美T1', 'T2', 'T3'];
    const promises = regionKeys.map(region =>
      fetch(getAdsJsonPath(p.folder, p.app_id, region)).then(r => r.ok ? r.json() : { ad_units: [] })
    );
    Promise.all(promises).then(results => {
      const rows = [];
      results.forEach((data, i) => {
        const regionKey = regionKeys[i];
        const regionLabel = (CREATIVE_REGIONS.find(r => r.key === regionKey) || {}).label || regionKey;
        const units = data.ad_units || [];
        units.forEach(unit => {
          const firstSeen = unit.first_seen_at || '';
          const lastSeen = unit.last_seen_at || '';
          const share = unit.share != null ? unit.share : '';
          const creatives = unit.creatives || [];
          creatives.forEach(c => {
            const url = c.creative_url || '';
            const durationSec = c.video_duration != null ? c.video_duration : '';
            const w = c.width; const h = c.height;
            const ratio = (w && h) ? (w + 'x' + h) : '';
            const orientation = (w != null && h != null) ? (w > h ? '横屏' : '竖屏') : '';
            let durationDays = '';
            let durationDaysNum = null;
            if (firstSeen && lastSeen) {
              const a = new Date(firstSeen); const b = new Date(lastSeen);
              durationDaysNum = Math.max(0, Math.round((b - a) / (24 * 60 * 60 * 1000)));
              durationDays = durationDaysNum + '天';
            }
            const firstSeenFmt = firstSeen ? firstSeen.replace(/-/g, '/') : '—';
            const tagLabel = CREATIVE_TAG_OPTIONS[Math.floor(Math.random() * CREATIVE_TAG_OPTIONS.length)];
            rows.push({
              creativeUrl: url,
              firstSeen: firstSeenFmt,
              durationDays: durationDays,
              durationDaysNum: durationDaysNum,
              share: share,
              region: regionLabel,
              regionKey: regionKey,
              tags: tagLabel,
              videoDuration: durationSec,
              ratio: ratio,
              orientation: orientation
            });
          });
        });
      });
      var dedupKey = function (r) {
        return (r.creativeUrl || '') + '|' + (r.firstSeen || '') + '|' + (r.durationDays || '') + '|' + (r.share !== '' && r.share != null ? String(r.share) : '') + '|' + (r.region || '');
      };
      var seen = {};
      var deduped = [];
      rows.forEach(function (r) {
        var k = dedupKey(r);
        if (!seen[k]) { seen[k] = true; deduped.push(r); }
      });
      creativeRows = deduped;
      applyCreativeFilters();
      loadProductDataForCreative(p);
      hide(loadingEl); hide(emptyEl); hide(errorEl);
      renderCreativeTableIfNeeded();
    }).catch(err => {
      errorEl.textContent = '加载创意数据失败: ' + (err.message || '');
      setState('error');
    });
  }

  function loadProductDataForCreative(p) {
    const key = creativeProductTable.value === 'strategy_new' ? 'new' : 'old';
    const url = base + '/data/' + currentYear + '/' + currentWeek + '/product_strategy_' + key + '.json';
    fetch(url).then(r => r.ok ? r.json() : {}).then(data => {
      const headers = data.headers || [];
      const rows = data.rows || [];
      const col = headers.indexOf('产品归属');
      const productName = (p.product_name || '').trim();
      const found = rows.find(row => {
        const cell = row[col];
        if (cell == null) return false;
        return productNamesMatch(productName, String(cell), String(cell));
      });
      productDataForCreative = found && headers.length ? { headers, row: found } : null;
      if (productDataForCreative) {
        creativeProductDataRow.style.display = 'block';
        creativeProductDataHead.innerHTML = '';
        creativeProductDataBody.innerHTML = '';
        headers.forEach((h, i) => {
          const th = document.createElement('th');
          th.textContent = h;
          creativeProductDataHead.appendChild(th);
        });
        headers.forEach((h, i) => {
          const td = document.createElement('td');
          const val = productDataForCreative.row[i];
          td.textContent = formatCell(h, val);
          if (h === '产品归属') td.classList.add('creative-product-name-cell');
          creativeProductDataBody.appendChild(td);
        });
      } else {
        creativeProductDataRow.style.display = 'none';
      }
    }).catch(() => { productDataForCreative = null; creativeProductDataRow.style.display = 'none'; });
  }

  function applyCreativeFilters() {
    let list = creativeRows || [];
    const regionVal = (creativeRegionFilter && creativeRegionFilter.value) || '';
    if (regionVal) list = list.filter(r => r.regionKey === regionVal);
    const durationVal = (creativeDurationFilter && creativeDurationFilter.value) || '';
    if (durationVal === 'under15') list = list.filter(r => r.durationDaysNum != null && r.durationDaysNum < 15);
    else if (durationVal === '15-30') list = list.filter(r => r.durationDaysNum != null && r.durationDaysNum >= 15 && r.durationDaysNum <= 30);
    else if (durationVal === 'over30') list = list.filter(r => r.durationDaysNum != null && r.durationDaysNum > 30);
    const ratioVal = (creativeRatioFilter && creativeRatioFilter.value) || '';
    if (ratioVal === '横屏') list = list.filter(r => r.orientation === '横屏');
    else if (ratioVal === '竖屏') list = list.filter(r => r.orientation === '竖屏');
    const q = (creativeSearchInput && creativeSearchInput.value || '').trim().toLowerCase();
    if (q) list = list.filter(r => (r.tags + ' ' + r.firstSeen + ' ' + r.region).toLowerCase().includes(q));
    creativeFilteredRows = list;
  }

  function renderCreativeTableIfNeeded() {
    if (currentDimension !== 'creative') return;
    tableHead.innerHTML = '';
    tableBody.innerHTML = '';
    const headers = ['素材视频', '首次看到', '持续时间', '展示份额', '投放地区', '素材标签'];
    const theadTr = document.createElement('tr');
    headers.forEach(h => {
      const th = document.createElement('th');
      th.textContent = h;
      theadTr.appendChild(th);
    });
    tableHead.appendChild(theadTr);

    const rows = creativeFilteredRows || [];
    rows.forEach(r => {
      const tr = document.createElement('tr');
      const tdVideo = document.createElement('td');
      tdVideo.className = 'creative-video-cell';
      if (r.creativeUrl) {
        const video = document.createElement('video');
        video.src = creativeVideoSrc(r.creativeUrl);
        video.controls = true;
        video.preload = 'metadata';
        video.setAttribute('playsinline', '');
        tdVideo.appendChild(video);
      } else {
        tdVideo.textContent = '—';
      }
      tr.appendChild(tdVideo);
      tr.appendChild(createTd(formatCell('首次看到', r.firstSeen)));
      tr.appendChild(createTd(r.durationDays || '—'));
      tr.appendChild(createTd(r.share !== '' && r.share != null ? formatCell('展示份额', r.share) : '—'));
      tr.appendChild(createTd(r.region || '—'));
      tr.appendChild(createTd(r.tags || '—'));
      tableBody.appendChild(tr);
    });
    hide(loadingEl); hide(errorEl);
    if (!creativeProductSelect.value && creativeProductsIndex) {
      show(emptyEl);
      emptyEl.textContent = '请选择产品';
    } else if (rows.length === 0 && (creativeRows || []).length === 0) {
      show(emptyEl);
      emptyEl.textContent = '本周期该产品暂无素材数据。可尝试从左侧切换周期（如 2026-0119-0125）查看其他周数据。';
    } else if (rows.length === 0 && (creativeRows || []).length > 0) {
      show(emptyEl);
      emptyEl.textContent = '无匹配素材';
    } else {
      hide(emptyEl);
    }
  }

  function createTd(text) {
    const td = document.createElement('td');
    td.textContent = text != null && text !== '' ? String(text) : '—';
    return td;
  }

  function setActiveWeekInSidebar(year, week) {
    document.querySelectorAll('.sidebar-week-link').forEach(a => {
      a.classList.remove('active');
      if (a.dataset.year === year && a.dataset.week === week) a.classList.add('active');
    });
  }

  function switchDimension(dim) {
    currentDimension = dim;
    if (window.location.hash !== '#' + dim) window.location.hash = '#' + dim;
    document.querySelectorAll('.top-nav-link').forEach(l => { l.classList.toggle('active', l.dataset.dim === dim); });
    if (dim === 'company') {
      show(panelCompany);
      hide(panelProduct);
      hide(panelCreative);
      hide(panelCombo);
      show(tableWrap);
      if (currentYear && currentWeek) loadCompanyWeek(currentYear, currentWeek);
      else setState('empty');
    } else if (dim === 'product') {
      hide(panelCompany);
      show(panelProduct);
      hide(panelCreative);
      hide(panelCombo);
      show(tableWrap);
      if (currentYear && currentWeek) loadProductWeek(currentYear, currentWeek);
      else setState('empty');
    } else if (dim === 'creative') {
      hide(panelCompany);
      hide(panelProduct);
      hide(panelCombo);
      show(panelCreative);
      show(tableWrap);
      if (currentYear && currentWeek) loadCreativeWeek(currentYear, currentWeek);
      else {
        creativeTitleEl.textContent = '请从左侧选择周期';
        creativeProductSelect.innerHTML = '<option value="">-- 请先选择周期 --</option>';
        creativeProductDataRow.style.display = 'none';
        setState('empty');
      }
    } else if (dim === 'combo') {
      hide(panelCompany);
      hide(panelProduct);
      hide(panelCreative);
      show(panelCombo);
      hide(tableWrap);
      hide(loadingEl);
      hide(emptyEl);
      hide(errorEl);
      updateComboDisplay();
    } else {
      hide(panelCompany);
      show(panelProduct);
      hide(panelCreative);
      hide(panelCombo);
      show(tableWrap);
      setState('empty');
    }
  }

  function updateComboDisplay() {
    comboTitleEl.textContent = '组合分析';
    comboPeriodEl.textContent = '--';
    var comboDeveloping = document.getElementById('comboDeveloping');
    var comboToolbar = document.querySelector('#panelCombo .combo-toolbar');
    var comboCharts = document.querySelector('#panelCombo .combo-charts');
    if (comboDeveloping) show(comboDeveloping);
    if (comboToolbar) hide(comboToolbar);
    if (comboCharts) hide(comboCharts);
    if (currentYear && currentWeek) setActiveWeekInSidebar(currentYear, currentWeek);
  }

  function buildSidebar(index) {
    weeksIndex = index;
    const years = Object.keys(index).sort((a, b) => Number(b) - Number(a));
    if (years.length && index[years[0]] && index[years[0]].length) {
      currentYear = years[0];
      currentWeek = index[years[0]][0];
    }
    let html = '';
    years.forEach(year => {
      const weeks = index[year] || [];
      const byMonth = {};
      weeks.forEach(weekTag => {
        const monthNum = parseInt(weekTag.substring(0, 2), 10);
        if (!byMonth[monthNum]) byMonth[monthNum] = [];
        byMonth[monthNum].push(weekTag);
      });
      const months = Object.keys(byMonth).map(Number).sort((a, b) => a - b);
      html += '<div class="sidebar-year"><div class="sidebar-year-title">' + year + '年</div>';
      months.forEach(m => {
        html += '<div class="sidebar-month-title">' + MONTH_NAMES[m - 1] + '</div>';
        html += '<ul class="sidebar-week-list">';
        byMonth[m].forEach(weekTag => {
          const active = currentYear === year && currentWeek === weekTag ? ' active' : '';
          html += '<li><a href="#" class="sidebar-week-link' + active + '" data-year="' + year + '" data-week="' + weekTag + '">' + weekTag + '</a></li>';
        });
        html += '</ul>';
      });
      html += '</div>';
    });
    sidebarYearsEl.innerHTML = html;

    sidebarYearsEl.querySelectorAll('.sidebar-week-link').forEach(a => {
      a.addEventListener('click', function (e) {
        e.preventDefault();
        loadWeek(this.dataset.year, this.dataset.week);
      });
    });

    if (years.length && weeksIndex[years[0]] && weeksIndex[years[0]].length) {
      currentYear = years[0];
      currentWeek = weeksIndex[years[0]][0];
      switchDimension(currentDimension);
    } else {
      setState('empty');
      switchDimension(currentDimension);
    }
  }

  function loadWeeksIndex() {
    fetch(base + '/data/weeks_index.json')
      .then(r => r.json())
      .then(buildSidebar)
      .catch(() => {
        sidebarYearsEl.innerHTML = '<p class="empty">无周期数据</p>';
        setState('empty');
      });
  }

  function downloadTable() {
    if (!currentData || !filteredRows || !filteredRows.length) return;
    const headers = currentData.headers;
    const rows = filteredRows.map(row => row.map((c, colIdx) => {
      const s = formatCell(headers[colIdx], c);
      return (s.includes(',') || s.includes('"') || s.includes('\n') ? '"' + String(s).replace(/"/g, '""') + '"' : s);
    }).join(','));
    const csv = '\uFEFF' + headers.join(',') + '\n' + rows.join('\n');
    const blob = new Blob([csv], { type: 'text/csv;charset=utf-8' });
    const a = document.createElement('a');
    a.href = URL.createObjectURL(blob);
    const suffix = currentDimension === 'product' ? '_爆量产品地区数据' : '_SLG数据监测表';
    a.download = (currentYear || 'data') + '_' + (currentWeek || 'table') + suffix + '.csv';
    a.click();
    URL.revokeObjectURL(a.href);
  }

  searchInput.addEventListener('input', filterRows);
  productSearchInput.addEventListener('input', filterRows);
  btnDownload.addEventListener('click', downloadTable);
  btnProductDownload.addEventListener('click', downloadTable);

  productSelect.addEventListener('change', function () {
    if (currentDimension === 'product' && currentYear && currentWeek) loadProductWeek(currentYear, currentWeek);
  });

  creativeProductTable.addEventListener('change', function () {
    if (currentDimension !== 'creative' || !creativeProductsIndex) return;
    fillCreativeProductSelect();
    creativeProductSelect.value = '';
    creativeProductDataRow.style.display = 'none';
    creativeRows = null;
    creativeFilteredRows = null;
    renderCreativeTableIfNeeded();
  });

  creativeProductSelect.addEventListener('change', function () {
    if (currentDimension !== 'creative') return;
    if (this.value) loadCreativeProductAds();
    else {
      creativeRows = null;
      creativeFilteredRows = null;
      creativeProductDataRow.style.display = 'none';
      renderCreativeTableIfNeeded();
    }
  });

  if (creativeRegionFilter) creativeRegionFilter.addEventListener('change', function () {
    applyCreativeFilters();
    renderCreativeTableIfNeeded();
  });
  if (creativeDurationFilter) creativeDurationFilter.addEventListener('change', function () {
    applyCreativeFilters();
    renderCreativeTableIfNeeded();
  });
  if (creativeRatioFilter) creativeRatioFilter.addEventListener('change', function () {
    applyCreativeFilters();
    renderCreativeTableIfNeeded();
  });
  if (creativeSearchInput) creativeSearchInput.addEventListener('input', function () {
    applyCreativeFilters();
    renderCreativeTableIfNeeded();
  });

  btnCreativeDownload.addEventListener('click', function () {
    if (currentDimension !== 'creative' || !creativeFilteredRows || !creativeFilteredRows.length) return;
    const headers = ['素材视频', '首次看到', '持续时间', '展示份额', '投放地区', '素材标签'];
    const rows = creativeFilteredRows.map(r => [
      r.creativeUrl || '',
      formatCell('首次看到', r.firstSeen),
      r.durationDays || '',
      r.share !== '' && r.share != null ? formatCell('展示份额', r.share) : '',
      r.region || '',
      (r.tags || '').replace(/"/g, '""')
    ]);
    const csv = '\uFEFF' + headers.join(',') + '\n' + rows.map(row => row.map(c => (c.includes(',') || c.includes('"') || c.includes('\n') ? '"' + c + '"' : c)).join(',')).join('\n');
    const blob = new Blob([csv], { type: 'text/csv;charset=utf-8' });
    const a = document.createElement('a');
    a.href = URL.createObjectURL(blob);
    a.download = (currentYear || '') + '_' + (currentWeek || '') + '_素材维度.csv';
    a.click();
    URL.revokeObjectURL(a.href);
  });

  tableBody.addEventListener('click', function (e) {
    // e.target 可能是 <a> 内的文本节点，文本节点无 .closest，需从父元素查找
    var start = e.target && e.target.nodeType === 1 ? e.target : (e.target && e.target.parentElement);
    var link = start && start.closest ? start.closest('a.product-link') : null;
    if (link) {
      e.preventDefault();
      pendingCreativeProduct = link.textContent.trim();
      switchDimension('creative');
      return;
    }
    var tr = start && start.closest ? start.closest('tr') : null;
    if (tr && tr.classList.contains('target-product-row') && tr.dataset.product) {
      e.preventDefault();
      pendingCreativeProduct = tr.dataset.product;
      switchDimension('creative');
    }
  });

  document.querySelectorAll('.top-nav-link').forEach(link => {
    link.addEventListener('click', function (e) {
      e.preventDefault();
      switchDimension(this.dataset.dim);
    });
  });

  document.querySelectorAll('.sub-nav-link').forEach(link => {
    link.addEventListener('click', function (e) {
      e.preventDefault();
      currentCompanySubTab = this.dataset.tab || 'overall';
      document.querySelectorAll('.sub-nav-link').forEach(l => l.classList.remove('active'));
      this.classList.add('active');
      if (currentDimension === 'company') showCompanySubTabContent();
    });
  });

  setInterval(updateLocalTime, 1000);
  updateLocalTime();
  loadWeeksIndex();
})();
