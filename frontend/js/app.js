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
  const panelCompanyDetail = document.getElementById('panelCompanyDetail');
  const companyHomeMarketWrap = document.getElementById('companyHomeMarketWrap');
  const companyHomeDetailPlaceholder = document.getElementById('companyHomeDetailPlaceholder');
  const panelProduct = document.getElementById('panelProduct');
  const panelProductDetail = document.getElementById('panelProductDetail');
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
  let productThemeStyleMapping = null;  // { byUnifiedId: { id: { 题材, 画风 } } }，来自 product_theme_style_mapping.json（mapping/产品归属.xlsx）
  let dataRange = null;  // { start: "YYYY-MM-DD", end: "YYYY-MM-DD" }，由 weeks_index.json 的 data_range 提供，跑完脚本后自动更新
  let currentYear = null;
  let currentWeek = null;
  let currentDimension = 'company';
  let currentCompanySubTab = 'overall';
  (function () {
    var h = (window.location.hash || '#company').replace('#', '').toLowerCase();
    if (['company', 'company-detail', 'product', 'product-detail', 'creative', 'combo'].indexOf(h) >= 0) currentDimension = h;
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
  let selectedProductForDetail = null;  // 从产品维度点击产品进入产品详细看板时：{ name, key: 'old'|'new' }
  let productDetailOrigin = 'product';  // 进入产品详细看板时的来源：'company' 从公司大盘点入，'product' 从产品大盘点入；用于「大盘数据」按钮跳回对应维度
  let selectedCompanyForDetail = null;  // 从公司维度大盘数据点击公司归属进入公司详细看板时：公司名
  let productDetailLineChartInstance = null;
  let productDetailStackedBarChartInstance = null;
  let companyDetailLineChartInstance = null;
  let companyDetailStackedBarChartInstance = null;
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

  /** 产品名精确匹配（规范化后完全一致），用于累计安装/流水等易错配场景，避免误取其他产品数据 */
  function productNameExactMatch(pending, name) {
    if (!pending || !name) return false;
    return normalizeProductName(pending) === normalizeProductName(name);
  }

  const MONTH_NAMES = ['01月', '02月', '03月', '04月', '05月', '06月', '07月', '08月', '09月', '10月', '11月', '12月'];

  function show(el) { el.style.display = ''; }
  function hide(el) { el.style.display = 'none'; }

  function setState(state) {
    if (loadingEl) hide(loadingEl);
    if (emptyEl) hide(emptyEl);
    if (errorEl) hide(errorEl);
    if (tableHead) tableHead.innerHTML = '';
    if (tableBody) tableBody.innerHTML = '';
    if (state === 'loading' && loadingEl) show(loadingEl);
    else if (state === 'empty' && emptyEl) show(emptyEl);
    else if (state === 'error' && errorEl) show(errorEl);
  }

  function updateLocalTime() {
    if (!localTimeEl) return;
    const now = new Date();
    const s = now.getFullYear() + '/' +
      String(now.getMonth() + 1).padStart(2, '0') + '/' +
      String(now.getDate()).padStart(2, '0') + ' ' +
      String(now.getHours()).padStart(2, '0') + ':' +
      String(now.getMinutes()).padStart(2, '0') + ':' +
      String(now.getSeconds()).padStart(2, '0');
    localTimeEl.textContent = '本机时间: ' + s;
  }

  const SUMMARY_ROW_BG = '#D9E1F2';

  function applyCellStyle(td, style, isSummaryRowCell) {
    if (!style) return;
    var bg = style.bg_color && !String(style.bg_color).startsWith('#Values') ? style.bg_color : null;
    if (bg && isSummaryRowCell) {
      var b = String(bg).toLowerCase().replace('#', '');
      if (b === 'fff2cc') bg = SUMMARY_ROW_BG;
    }
    if (bg) td.style.backgroundColor = bg;
    if (style.font_color && String(style.font_color).startsWith('#')) td.style.color = style.font_color;
    if (style.bold) td.style.fontWeight = 'bold';
  }

  function isRedFont(style) {
    if (!style || !style.font_color) return false;
    var c = String(style.font_color).toLowerCase().replace('#', '');
    return c === 'ff0000';
  }

  /** 仅黄底 #FFF2CC 表示目标产品行；浅蓝为汇总行，不算目标产品 */
  function isTargetProductRow(styleRow) {
    if (!styleRow || !Array.isArray(styleRow)) return false;
    return styleRow.some(function (s) {
      if (!s || !s.bg_color) return false;
      var bg = String(s.bg_color).toLowerCase().replace('#', '');
      return bg === 'fff2cc';
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
      if (s.includes('%') && (s.includes('▲') || s.includes('▼'))) {
        // 与公司维度统一：去掉前导 + 号
        return s.replace(/^\s*\+/, '');
      }
      if (isNum) {
        const pct = (n * 100).toFixed(2);
        const arrow = n >= 0 ? '▲' : '▼';
        return pct + '%' + arrow;
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
    const companyColIndexInitial = headers.indexOf('公司归属');
    const unifiedIdColIndexInitial = headers.indexOf('Unified ID');
    const isCompanyDimInitial = currentDimension === 'company';
    headers.forEach((h, i) => {
      if (isCompanyDimInitial && h === 'Unified ID') return;
      const th = document.createElement('th');
      th.textContent = h;
      th.dataset.col = i;
      th.dataset.sort = '';
      if (styles[0] && styles[0][i]) applyCellStyle(th, styles[0][i], false);
      if (!isCompanyDimInitial) th.addEventListener('click', () => sortBy(i));
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
      if (isCompanyDimInitial && !isSummaryRow(row) && unifiedIdColIndexInitial >= 0 && row[unifiedIdColIndexInitial] != null && String(row[unifiedIdColIndexInitial]).trim() !== '') {
        tr.dataset.unifiedId = String(row[unifiedIdColIndexInitial]).trim();
      }
      row.forEach((cell, colIdx) => {
        if (isCompanyDimInitial && colIdx === unifiedIdColIndexInitial) return;
        const td = document.createElement('td');
        const isCompanyNameLink = isCompanyDimInitial && !isSummaryRow(row) && colIdx === companyColIndexInitial && companyColIndexInitial >= 0 && cell != null && String(cell) !== '';
        const isProductNameLink = (isProductDimension || (!isSummaryRow(row) && isCompanyDimInitial)) && colIdx === productColIndex && productColIndex >= 0;
        if (isCompanyNameLink) {
          const a = document.createElement('a');
          a.href = '#';
          a.className = 'company-link';
          a.textContent = String(cell);
          td.appendChild(a);
        } else if (isProductNameLink && cell != null && String(cell) !== '') {
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
        if (headers[colIdx] === '周安装变动') td.classList.add('cell-change-pct');
        if (styleRow && styleRow[colIdx]) applyCellStyle(td, styleRow[colIdx], isSummaryRow(row));
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

    document.querySelectorAll('#tableHead th').forEach(function (th) {
      var origCol = th.dataset.col != null ? parseInt(th.dataset.col, 10) : -1;
      th.dataset.sort = origCol === colIndex ? (sortAsc ? 'asc' : 'desc') : '';
    });
    tableBody.innerHTML = '';
    const styles = currentData.styles || [];
    const productColIndex = currentData.headers.indexOf('产品归属');
    const companyColIndex = currentData.headers.indexOf('公司归属');
    const unifiedIdColIndex = currentData.headers.indexOf('Unified ID');
    const isCompanyDim = currentDimension === 'company';
    filteredRows.forEach((row, idx) => {
      const tr = document.createElement('tr');
      if (isSummaryRow(row)) tr.classList.add('row-summary');
      const origIdx = currentData.rows.indexOf(row);
      const styleRow = origIdx >= 0 ? styles[origIdx + 1] : styles[idx + 1];
      if (currentDimension === 'company' && !isSummaryRow(row) && isTargetProductRow(styleRow) && productColIndex >= 0 && row[productColIndex] != null) {
        tr.classList.add('target-product-row');
        tr.dataset.product = String(row[productColIndex]);
      }
      if (currentDimension === 'product' && !isSummaryRow(row) && unifiedIdColIndex >= 0 && row[unifiedIdColIndex] != null && String(row[unifiedIdColIndex]).trim() !== '') {
        tr.dataset.unifiedId = String(row[unifiedIdColIndex]).trim();
      }
      if (currentDimension === 'company' && !isSummaryRow(row) && unifiedIdColIndex >= 0 && row[unifiedIdColIndex] != null && String(row[unifiedIdColIndex]).trim() !== '') {
        tr.dataset.unifiedId = String(row[unifiedIdColIndex]).trim();
      }
      const isProductDim = currentDimension === 'product';
      row.forEach((cell, colIdx) => {
        if (isCompanyDim && colIdx === unifiedIdColIndex) return;
        const td = document.createElement('td');
        const isCompanyNameLink = isCompanyDim && !isSummaryRow(row) && colIdx === companyColIndex && companyColIndex >= 0 && cell != null && String(cell) !== '';
        const isProductNameLink = (isProductDim || (!isSummaryRow(row) && isCompanyDim)) && colIdx === productColIndex && productColIndex >= 0;
        if (isCompanyNameLink) {
          const a = document.createElement('a');
          a.href = '#';
          a.className = 'company-link';
          a.textContent = String(cell);
          td.appendChild(a);
        } else if (isProductNameLink && cell != null && String(cell) !== '') {
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
        if (currentData.headers[colIdx] === '周安装变动') td.classList.add('cell-change-pct');
        if (styleRow && styleRow[colIdx]) applyCellStyle(td, styleRow[colIdx], isSummaryRow(row));
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
        hide(loadingEl);
        hide(errorEl);
        if (currentCompanySubTab === 'overall') {
          renderTable(false);
          hide(emptyEl);
        } else {
          showCompanySubTabContent();
        }
      })
      .catch(err => { errorEl.textContent = '加载失败: ' + err.message; setState('error'); });
  }

  function showCompanySubTabContent() {
    if (currentCompanySubTab === 'detail') {
      hide(tableWrap);
      hide(companyHomeMarketWrap);
      hide(loadingEl);
      hide(errorEl);
      hide(emptyEl);
      if (companyHomeDetailPlaceholder) show(companyHomeDetailPlaceholder);
    } else {
      if (companyHomeDetailPlaceholder) hide(companyHomeDetailPlaceholder);
      show(companyHomeMarketWrap);
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
    productPeriodEl.textContent = dataRange ? formatDataRangeDisplay(dataRange) : week;
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
    else if (currentDimension === 'company-detail') switchDimension('company');
    else if (currentDimension === 'product') loadProductWeek(year, week);
    else if (currentDimension === 'product-detail') switchDimension('product');
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
    if (dim !== 'product-detail') destroyProductDetailCharts();
    if (dim !== 'company-detail') destroyCompanyTrendCharts();
    if (window.location.hash !== '#' + dim) window.location.hash = '#' + dim;
    var navDim = dim === 'product-detail' ? 'product' : (dim === 'company-detail' ? 'company' : dim);
    document.querySelectorAll('.top-nav-link').forEach(l => { l.classList.toggle('active', l.dataset.dim === navDim); });
    if (dim === 'company') {
      show(panelCompany);
      hide(panelCompanyDetail);
      hide(panelProduct);
      hide(panelProductDetail);
      hide(panelCreative);
      hide(panelCombo);
      showCompanySubTabContent();
      if (currentYear && currentWeek) loadCompanyWeek(currentYear, currentWeek);
      else setState('empty');
    } else if (dim === 'company-detail') {
      hide(panelCompany);
      show(panelCompanyDetail);
      hide(panelProduct);
      hide(panelProductDetail);
      hide(panelCreative);
      hide(panelCombo);
      hide(tableWrap);
      hide(loadingEl);
      hide(emptyEl);
      hide(errorEl);
      var companyDetailSubNav = document.getElementById('companyDetailSubNav');
      if (companyDetailSubNav) {
        companyDetailSubNav.querySelectorAll('.company-detail-sub-link').forEach(function (l) { l.classList.remove('active'); });
        var detailTab = companyDetailSubNav.querySelector('.company-detail-sub-link[data-tab="detail"]');
        if (detailTab) detailTab.classList.add('active');
      }
      if (currentYear && currentWeek && selectedCompanyForDetail) loadCompanyDetail();
      else renderCompanyDetailPlaceholder();
    } else if (dim === 'product') {
      hide(panelCompany);
      hide(panelCompanyDetail);
      show(panelProduct);
      hide(panelProductDetail);
      hide(panelCreative);
      hide(panelCombo);
      show(tableWrap);
      if (currentYear && currentWeek) loadProductWeek(currentYear, currentWeek);
      else setState('empty');
    } else if (dim === 'product-detail') {
      hide(panelCompany);
      hide(panelCompanyDetail);
      hide(panelProduct);
      show(panelProductDetail);
      hide(panelCreative);
      hide(panelCombo);
      hide(tableWrap);
      hide(loadingEl);
      hide(emptyEl);
      hide(errorEl);
      document.querySelectorAll('.product-detail-sub-link').forEach(function (l) { l.classList.remove('active'); });
      var productDetailTab = document.querySelector('.product-detail-sub-link[data-tab="detail"]');
      if (productDetailTab) productDetailTab.classList.add('active');
      if (currentYear && currentWeek && selectedProductForDetail) loadProductDetail();
      else renderProductDetailPlaceholder();
    } else if (dim === 'creative') {
      hide(panelCompany);
      hide(panelCompanyDetail);
      hide(panelProduct);
      hide(panelProductDetail);
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
      hide(panelCompanyDetail);
      hide(panelProduct);
      hide(panelProductDetail);
      hide(panelCreative);
      show(panelCombo);
      hide(tableWrap);
      hide(loadingEl);
      hide(emptyEl);
      hide(errorEl);
      updateComboDisplay();
    } else {
      hide(panelCompany);
      hide(panelCompanyDetail);
      show(panelProduct);
      hide(panelProductDetail);
      hide(panelCreative);
      hide(panelCombo);
      show(tableWrap);
      setState('empty');
    }
  }

  function weekTagToDateRange(year, weekTag) {
    if (!year || !weekTag) return '—';
    var m = String(weekTag).match(/^(\d{2})(\d{2})-(\d{2})(\d{2})$/);
    if (!m) return '—';
    return year + '年-' + m[1] + '月-' + m[2] + '日 ~ ' + year + '年-' + m[3] + '月-' + m[4] + '日';
  }

  /** 将当前选中的周 (year, weekTag) 转为「YYYY/MM/DD - YYYY/MM/DD」，用于产品/公司详情页显示「进入该页时对应的时间段」 */
  function weekTagToSlashDateRange(year, weekTag) {
    if (!year || !weekTag) return '—';
    var m = String(weekTag).match(/^(\d{2})(\d{2})-(\d{2})(\d{2})$/);
    if (!m) return '—';
    var y = parseInt(year, 10);
    var sm = m[1], sd = m[2], em = m[3], ed = m[4];
    var endYear = parseInt(em, 10) < parseInt(sm, 10) ? y + 1 : y;
    return y + '/' + sm + '/' + sd + ' - ' + endYear + '/' + em + '/' + ed;
  }

  /** 将 data_range（{ start, end }）格式化为「YYYY年MM月DD日 ~ YYYY年MM月DD日」，供界面显示「我们有数据的时间」 */
  function formatDataRangeDisplay(range) {
    if (!range || !range.start || !range.end) return '—';
    var s = String(range.start);
    var e = String(range.end);
    var sMatch = s.match(/^(\d{4})-(\d{2})-(\d{2})$/);
    var eMatch = e.match(/^(\d{4})-(\d{2})-(\d{2})$/);
    if (!sMatch || !eMatch) return '—';
    return sMatch[1] + '年' + sMatch[2] + '月' + sMatch[3] + '日 ~ ' + eMatch[1] + '年' + eMatch[2] + '月' + eMatch[3] + '日';
  }

  function renderProductDetailPlaceholder() {
    var titleEl = document.getElementById('productDetailTitle');
    var dateEl = document.getElementById('productDetailDateRange');
    if (titleEl) titleEl.textContent = '产品详细信息';
    if (dateEl) dateEl.textContent = '—';
    ['productDetailCompany', 'productDetailNewOld', 'productDetailLaunch', 'productDetailTheme', 'productDetailStyle', 'productDetailInstall', 'productDetailRankInstall', 'productDetailRevenue', 'productDetailRankRevenue'].forEach(function (id) {
      var el = document.getElementById(id);
      if (el) el.textContent = '—';
    });
  }

  function renderCompanyDetailPlaceholder() {
    var titleEl = document.getElementById('companyDetailTitle');
    var dateEl = document.getElementById('companyDetailDateRange');
    var tableDateEl = document.getElementById('companyDetailTableDateRange');
    if (titleEl) titleEl.textContent = '公司详细信息';
    if (dateEl) dateEl.textContent = '—';
    if (tableDateEl) tableDateEl.textContent = '—';
    ['companyDetailInstall', 'companyDetailRankInstall', 'companyDetailRevenue', 'companyDetailRankRevenue'].forEach(function (id) {
      var el = document.getElementById(id);
      if (el) el.textContent = '—';
    });
    var tbody = document.getElementById('companyDetailProductBody');
    if (tbody) tbody.innerHTML = '';
  }

  function loadCompanyDetail() {
    if (!currentYear || !currentWeek || !selectedCompanyForDetail) {
      renderCompanyDetailPlaceholder();
      return;
    }
    var titleEl = document.getElementById('companyDetailTitle');
    var dateEl = document.getElementById('companyDetailDateRange');
    var tableDateEl = document.getElementById('companyDetailTableDateRange');
    if (titleEl) titleEl.textContent = selectedCompanyForDetail;
    var dateRangeText = dataRange ? formatDataRangeDisplay(dataRange) : weekTagToDateRange(currentYear, currentWeek);
    if (dateEl) dateEl.textContent = dateRangeText;
    if (tableDateEl) tableDateEl.textContent = dateRangeText;
    ['companyDetailInstall', 'companyDetailRankInstall', 'companyDetailRevenue', 'companyDetailRankRevenue'].forEach(function (id) {
      var el = document.getElementById(id);
      if (el) el.textContent = '—';
    });
    var url = base + '/data/' + currentYear + '/' + currentWeek + '_formatted.json';
    fetch(url)
      .then(function (r) { if (!r.ok) throw new Error(r.statusText || '加载失败'); return r.json(); })
      .then(function (data) {
        var headers = data.headers || [];
        var rows = data.rows || [];
        var companyColIdx = headers.indexOf('公司归属');
        var productColIdx = headers.indexOf('产品归属');
        var unifiedIdColIdx = headers.indexOf('Unified ID');
        var launchColIdx = headers.indexOf('第三方记录最早上线时间');
        var installChangeColIdx = headers.indexOf('周安装变动');
        var revenueChangeColIdx = headers.indexOf('周流水变动');
        var companyRows = companyColIdx >= 0 ? rows.filter(function (r) {
          var c = r[companyColIdx];
          return c != null && String(c).trim() === String(selectedCompanyForDetail).trim();
        }) : [];
        var tbody = document.getElementById('companyDetailProductBody');
        if (!tbody) return;
        tbody.innerHTML = '';
        companyRows.forEach(function (r) {
          var tr = document.createElement('tr');
          var productName = productColIdx >= 0 ? (r[productColIdx] != null ? String(r[productColIdx]) : '—') : '—';
          var launch = launchColIdx >= 0 ? (r[launchColIdx] != null ? String(r[launchColIdx]).replace(/\s+\d{2}:\d{2}:\d{2}$/, '') : '—') : '—';
          var installChange = installChangeColIdx >= 0 ? (r[installChangeColIdx] != null ? formatCell('周安装变动', r[installChangeColIdx]) : '—') : '—';
          var revenueChange = revenueChangeColIdx >= 0 ? (r[revenueChangeColIdx] != null ? formatCell('周流水变动', r[revenueChangeColIdx]) : '—') : '—';
          tr.innerHTML = '<td>' + escapeHtml(productName) + '</td><td>' + escapeHtml(launch) + '</td><td>' + escapeHtml(installChange) + '</td><td>' + escapeHtml(revenueChange) + '</td>';
          tbody.appendChild(tr);
        });
        // 公司累计安装/流水 = 该公司下所有产品的累计安装/流水加和；赛道排名 = 按公司汇总后的安装/流水在全赛道中的名次
        function setCompanyCumulative(sumInstall, sumRevenue) {
          var installEl = document.getElementById('companyDetailInstall');
          var revenueEl = document.getElementById('companyDetailRevenue');
          if (installEl) {
            if (sumInstall != null && !Number.isNaN(sumInstall)) {
              installEl.textContent = Number(sumInstall).toLocaleString('en-US');
            } else {
              installEl.textContent = '—';
            }
          }
          if (revenueEl) {
            if (sumRevenue != null && !Number.isNaN(sumRevenue)) {
              revenueEl.textContent = '$' + Number(sumRevenue).toLocaleString('en-US', { minimumFractionDigits: 0, maximumFractionDigits: 0 });
            } else {
              revenueEl.textContent = '—';
            }
          }
        }
        function setCompanyRank(rankInstall, rankRevenue) {
          var rankInstallEl = document.getElementById('companyDetailRankInstall');
          var rankRevenueEl = document.getElementById('companyDetailRankRevenue');
          if (rankInstallEl) rankInstallEl.textContent = rankInstall != null && rankInstall > 0 ? '第' + rankInstall + '名' : '—';
          if (rankRevenueEl) rankRevenueEl.textContent = rankRevenue != null && rankRevenue > 0 ? '第' + rankRevenue + '名' : '—';
        }
        var pairs = [];
        if (weeksIndex) {
          Object.keys(weeksIndex).filter(function (y) { return /^\d{4}$/.test(y); }).forEach(function (year) {
            (weeksIndex[year] || []).forEach(function (weekTag) { pairs.push({ year: year, weekTag: weekTag }); });
          });
          pairs.sort(function (a, b) { return weekSortKey(b.year, b.weekTag) - weekSortKey(a.year, a.weekTag); });
        }
        function tryNextMetrics(i) {
          if (i >= pairs.length) {
            setCompanyCumulative(null, null);
            setCompanyRank(null, null);
            loadCompanyTrendCharts();
            return Promise.resolve();
          }
          var p = pairs[i];
          return fetch(base + '/data/' + p.year + '/' + p.weekTag + '/metrics_total.json')
            .then(function (r) { return r.ok ? r.json() : null; })
            .catch(function () { return null; })
            .then(function (metrics) {
              if (!metrics || !metrics.rows || !metrics.rows.length || !metrics.headers) return tryNextMetrics(i + 1);
              var mHeaders = metrics.headers;
              var mRows = metrics.rows;
              var mUnifiedCol = mHeaders.indexOf('Unified ID');
              var mProductCol = mHeaders.indexOf('产品归属');
              var mAllTimeDownloadsCol = mHeaders.indexOf('All Time Downloads (WW)');
              var mAllTimeRevenueCol = mHeaders.indexOf('All Time Revenue (WW)');
              if (mProductCol < 0 || (mAllTimeDownloadsCol < 0 && mAllTimeRevenueCol < 0)) return tryNextMetrics(i + 1);
              var sumInstall = 0;
              var sumRevenue = 0;
              for (var j = 0; j < companyRows.length; j++) {
                var r = companyRows[j];
                var productName = productColIdx >= 0 && r[productColIdx] != null ? String(r[productColIdx]).trim() : '';
                var unifiedId = unifiedIdColIdx >= 0 && r[unifiedIdColIdx] != null ? String(r[unifiedIdColIdx]).trim() : '';
                var installVal = null;
                var revenueVal = null;
                for (var k = 0; k < mRows.length; k++) {
                  var mr = mRows[k];
                  var match = false;
                  if (unifiedId && mUnifiedCol >= 0 && mr[mUnifiedCol] != null && String(mr[mUnifiedCol]).trim() === unifiedId) match = true;
                  if (!match && productName && mProductCol >= 0 && productNameExactMatch(productName, mr[mProductCol])) match = true;
                  if (match) {
                    if (mAllTimeDownloadsCol >= 0 && mr[mAllTimeDownloadsCol] != null && mr[mAllTimeDownloadsCol] !== '') installVal = mr[mAllTimeDownloadsCol];
                    if (mAllTimeRevenueCol >= 0 && mr[mAllTimeRevenueCol] != null && mr[mAllTimeRevenueCol] !== '') revenueVal = mr[mAllTimeRevenueCol];
                    break;
                  }
                }
                if (installVal != null && installVal !== '') {
                  var n = typeof installVal === 'number' ? installVal : (parseFloat(String(installVal).replace(/,/g, '')) || parseInt(String(installVal).replace(/,/g, ''), 10));
                  if (!Number.isNaN(n)) sumInstall += n;
                }
                if (revenueVal != null && revenueVal !== '') {
                  var rStr = String(revenueVal).replace(/[$,]/g, '');
                  var rn = typeof revenueVal === 'number' ? revenueVal : (parseFloat(rStr) || parseInt(rStr, 10));
                  if (!Number.isNaN(rn)) sumRevenue += rn;
                }
              }
              setCompanyCumulative(sumInstall, sumRevenue);
              // 赛道排名：按公司汇总累计安装/流水，在全赛道公司中排序得到名次
              var companyProductList = {};
              rows.forEach(function (r) {
                var company = r[companyColIdx];
                if (company == null || String(company).trim() === '') return;
                company = String(company).trim();
                var productName = productColIdx >= 0 && r[productColIdx] != null ? String(r[productColIdx]).trim() : '';
                var unifiedId = unifiedIdColIdx >= 0 && r[unifiedIdColIdx] != null ? String(r[unifiedIdColIdx]).trim() : '';
                if (!companyProductList[company]) companyProductList[company] = [];
                companyProductList[company].push({ productName: productName, unifiedId: unifiedId });
              });
              var companyTotals = {};
              Object.keys(companyProductList).forEach(function (companyName) {
                var list = companyProductList[companyName];
                var cInstall = 0;
                var cRevenue = 0;
                for (var jj = 0; jj < list.length; jj++) {
                  var item = list[jj];
                  var installVal = null;
                  var revenueVal = null;
                  for (var kk = 0; kk < mRows.length; kk++) {
                    var mr = mRows[kk];
                    var match = false;
                    if (item.unifiedId && mUnifiedCol >= 0 && mr[mUnifiedCol] != null && String(mr[mUnifiedCol]).trim() === item.unifiedId) match = true;
                    if (!match && item.productName && mProductCol >= 0 && productNameExactMatch(item.productName, mr[mProductCol])) match = true;
                    if (match) {
                      if (mAllTimeDownloadsCol >= 0 && mr[mAllTimeDownloadsCol] != null && mr[mAllTimeDownloadsCol] !== '') installVal = mr[mAllTimeDownloadsCol];
                      if (mAllTimeRevenueCol >= 0 && mr[mAllTimeRevenueCol] != null && mr[mAllTimeRevenueCol] !== '') revenueVal = mr[mAllTimeRevenueCol];
                      break;
                    }
                  }
                  if (installVal != null && installVal !== '') {
                    var nn = typeof installVal === 'number' ? installVal : (parseFloat(String(installVal).replace(/,/g, '')) || parseInt(String(installVal).replace(/,/g, ''), 10));
                    if (!Number.isNaN(nn)) cInstall += nn;
                  }
                  if (revenueVal != null && revenueVal !== '') {
                    var rrStr = String(revenueVal).replace(/[$,]/g, '');
                    var rrn = typeof revenueVal === 'number' ? revenueVal : (parseFloat(rrStr) || parseInt(rrStr, 10));
                    if (!Number.isNaN(rrn)) cRevenue += rrn;
                  }
                }
                companyTotals[companyName] = { install: cInstall, revenue: cRevenue };
              });
              var companiesByInstall = Object.keys(companyTotals).sort(function (a, b) { return companyTotals[b].install - companyTotals[a].install; });
              var companiesByRevenue = Object.keys(companyTotals).sort(function (a, b) { return companyTotals[b].revenue - companyTotals[a].revenue; });
              var selectedNorm = String(selectedCompanyForDetail).trim();
              var rankInstall = 0;
              var rankRevenue = 0;
              for (var ri = 0; ri < companiesByInstall.length; ri++) { if (String(companiesByInstall[ri]).trim() === selectedNorm) { rankInstall = ri + 1; break; } }
              for (var rr = 0; rr < companiesByRevenue.length; rr++) { if (String(companiesByRevenue[rr]).trim() === selectedNorm) { rankRevenue = rr + 1; break; } }
              setCompanyRank(rankInstall, rankRevenue);
              loadCompanyTrendCharts();
              return Promise.resolve();
            });
        }
        return tryNextMetrics(0);
      })
      .catch(function () { renderCompanyDetailPlaceholder(); });
  }

  function destroyCompanyTrendCharts() {
    if (companyDetailLineChartInstance) {
      companyDetailLineChartInstance.destroy();
      companyDetailLineChartInstance = null;
    }
    if (companyDetailStackedBarChartInstance) {
      companyDetailStackedBarChartInstance.destroy();
      companyDetailStackedBarChartInstance = null;
    }
  }

  var COMPANY_TREND_COLORS = [
    'rgba(52,152,219,0.8)', 'rgba(46,204,113,0.8)', 'rgba(241,196,15,0.8)', 'rgba(230,126,34,0.8)',
    'rgba(155,89,182,0.8)', 'rgba(26,188,156,0.8)', 'rgba(231,76,60,0.8)', 'rgba(149,165,166,0.8)',
    'rgba(52,73,94,0.8)', 'rgba(243,156,18,0.8)'
  ];

  function loadCompanyTrendCharts() {
    if (!selectedCompanyForDetail || !window.Chart) return;
    var loadingEl = document.getElementById('companyDetailTrendLoading');
    if (loadingEl) loadingEl.style.display = '';
    destroyCompanyTrendCharts();
    fetch(base + '/data/weeks_index.json')
      .then(function (r) { if (!r.ok) throw new Error(r.statusText); return r.json(); })
      .then(function (index) {
        var pairs = [];
        Object.keys(index || {}).filter(function (y) { return /^\d{4}$/.test(y); }).forEach(function (year) {
          (index[year] || []).forEach(function (weekTag) {
            pairs.push({ year: year, weekTag: weekTag });
          });
        });
        pairs.sort(function (a, b) {
          return weekSortKey(a.year, a.weekTag) - weekSortKey(b.year, b.weekTag);
        });
        var weekLabels = [];
        var companyInstalls = [];
        var allProductNames = [];
        var productData = {};
        var companyNorm = String(selectedCompanyForDetail || '').trim();
        function fetchNext(i) {
          if (i >= pairs.length) {
            if (loadingEl) loadingEl.style.display = 'none';
            renderCompanyTrendCharts(weekLabels, companyInstalls, allProductNames, productData);
            return;
          }
          var p = pairs[i];
          weekLabels.push(p.weekTag);
          var url = base + '/data/' + p.year + '/' + p.weekTag + '_formatted.json';
          fetch(url)
            .then(function (r) { return r.ok ? r.json() : { headers: [], rows: [] }; })
            .then(function (data) {
              var headers = data.headers || [];
              var rows = data.rows || [];
              var companyColIdx = headers.indexOf('公司归属');
              var productColIdx = headers.indexOf('产品归属');
              var installColIdx = headers.indexOf('当周周安装');
              var companySum = 0;
              var filtered = companyColIdx >= 0 && installColIdx >= 0 ? rows.filter(function (r) {
                var c = r[companyColIdx];
                return c != null && String(c).trim() === companyNorm;
              }) : [];
              var seenThisWeek = {};
              filtered.forEach(function (r) {
                var install = typeof r[installColIdx] === 'number' ? r[installColIdx] : (parseFloat(r[installColIdx]) || parseInt(r[installColIdx], 10) || 0);
                companySum += install;
                var productName = productColIdx >= 0 && r[productColIdx] != null ? String(r[productColIdx]).trim() : '';
                if (!productName) return;
                if (!productData[productName]) {
                  productData[productName] = new Array(i).fill(0);
                  allProductNames.push(productName);
                }
                productData[productName].push(install);
                seenThisWeek[productName] = true;
              });
              companyInstalls.push(companySum);
              allProductNames.forEach(function (name) {
                if (!seenThisWeek[name]) productData[name].push(0);
              });
              fetchNext(i + 1);
            })
            .catch(function () {
              companyInstalls.push(0);
              allProductNames.forEach(function (name) {
                productData[name].push(0);
              });
              fetchNext(i + 1);
            });
        }
        fetchNext(0);
      })
      .catch(function () {
        if (loadingEl) loadingEl.style.display = 'none';
      });
  }

  function renderCompanyTrendCharts(weekLabels, companyInstalls, allProductNames, productData) {
    if (!window.Chart) return;
    destroyCompanyTrendCharts();
    var lineCtx = document.getElementById('companyDetailLineChart');
    var barCtx = document.getElementById('companyDetailStackedBarChart');
    if (!lineCtx || !barCtx) return;
    companyDetailLineChartInstance = new Chart(lineCtx, {
      type: 'line',
      data: {
        labels: weekLabels,
        datasets: [{ label: '当周周安装（公司汇总）', data: companyInstalls, borderColor: '#3498db', backgroundColor: 'rgba(52,152,219,0.1)', fill: true, tension: 0.2 }]
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        plugins: { legend: { display: false } },
        scales: {
          x: { display: true, title: { display: false } },
          y: { beginAtZero: true, title: { display: true, text: '安装量' } }
        }
      }
    });
    var datasets = allProductNames.map(function (name, idx) {
      var color = COMPANY_TREND_COLORS[idx % COMPANY_TREND_COLORS.length];
      return { label: name, data: productData[name] || [], backgroundColor: color, stack: 'stack1' };
    });
    companyDetailStackedBarChartInstance = new Chart(barCtx, {
      type: 'bar',
      data: { labels: weekLabels, datasets: datasets },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        plugins: { legend: { position: 'top' } },
        scales: {
          x: { stacked: true, title: { display: false } },
          y: { stacked: true, beginAtZero: true, title: { display: true, text: '安装量' } }
        }
      }
    });
  }

  function escapeHtml(s) {
    if (s == null || s === '') return '';
    var div = document.createElement('div');
    div.textContent = s;
    return div.innerHTML;
  }

  function loadProductDetail() {
    if (!currentYear || !currentWeek || !selectedProductForDetail) {
      renderProductDetailPlaceholder();
      return;
    }
    var titleEl = document.getElementById('productDetailTitle');
    var dateEl = document.getElementById('productDetailDateRange');
    if (titleEl) titleEl.textContent = selectedProductForDetail.name || '产品详细信息';
    if (dateEl) dateEl.textContent = weekTagToSlashDateRange(currentYear, currentWeek);
    // 题材/画风只从一张表取：mapping/产品归属.xlsx → product_theme_style_mapping.json（服务器可注入 __PRODUCT_THEME_STYLE_MAPPING__）
    var dataBase = (document.location.pathname || '').indexOf('frontend') >= 0 ? '/frontend' : base;
    if (!productThemeStyleMapping && typeof window.__PRODUCT_THEME_STYLE_MAPPING__ !== 'undefined' && window.__PRODUCT_THEME_STYLE_MAPPING__ != null) productThemeStyleMapping = window.__PRODUCT_THEME_STYLE_MAPPING__;
    var ensureMapping = productThemeStyleMapping ? Promise.resolve() : fetch(dataBase + '/data/product_theme_style_mapping.json').then(function (r) { return r.ok ? r.json() : null; }).catch(function () { return null; }).then(function (m) { productThemeStyleMapping = m && (m.byUnifiedId || m.byProductName) ? m : { byUnifiedId: {}, byProductName: {} }; });
    // 同时拉取 old/new 两个 strategy，便于从公司维度等进入时也能解析出 Unified ID
    var urlOld = base + '/data/' + currentYear + '/' + currentWeek + '/product_strategy_old.json';
    var urlNew = base + '/data/' + currentYear + '/' + currentWeek + '/product_strategy_new.json';
    var urlFormatted = base + '/data/' + currentYear + '/' + currentWeek + '_formatted.json';
    ensureMapping.then(function () {
      return Promise.all([
        fetch(urlOld).then(function (r) { return r.ok ? r.json() : { headers: [], rows: [] }; }),
        fetch(urlNew).then(function (r) { return r.ok ? r.json() : { headers: [], rows: [] }; }),
        fetch(urlFormatted).then(function (r) { if (!r.ok) return { headers: [], rows: [] }; return r.json(); })
      ]);
    }).then(function (results) {
        var dataOld = results[0];
        var dataNew = results[1];
        var formattedData = results[2];
        var headers = (dataOld.headers && dataOld.headers.length) ? dataOld.headers : (dataNew.headers || []);
        var rows = (dataOld.rows || []).concat(dataNew.rows || []);
        var data = { headers: headers, rows: rows, dataOld: dataOld, dataNew: dataNew };
        var pairs = [];
        if (weeksIndex) {
          Object.keys(weeksIndex).filter(function (y) { return /^\d{4}$/.test(y); }).forEach(function (year) {
            (weeksIndex[year] || []).forEach(function (weekTag) { pairs.push({ year: year, weekTag: weekTag }); });
          });
          pairs.sort(function (a, b) { return weekSortKey(b.year, b.weekTag) - weekSortKey(a.year, a.weekTag); });
        }
        function tryNextMetrics(i) {
          if (i >= pairs.length) return runProductDetailLogic(data, formattedData, null);
          var p = pairs[i];
          return fetch(base + '/data/' + p.year + '/' + p.weekTag + '/metrics_total.json')
            .then(function (r) { return r.ok ? r.json() : null; })
            .catch(function () { return null; })
            .then(function (metrics) {
              if (!metrics || !metrics.rows || !metrics.rows.length || !metrics.headers) return tryNextMetrics(i + 1);
              // 累计与排名统一：用同一份 metrics_total，排名在 runProductDetailLogic 里按累计安装/流水排序算出
              return runProductDetailLogic(data, formattedData, metrics);
            });
        }
        return tryNextMetrics(0);
    }).then(function () {
      loadProductTrendCharts();
    })
      .catch(function () {
        renderProductDetailPlaceholder();
      });
  }

  function runProductDetailLogic(data, formattedData, metricsData) {
        var dateEl = document.getElementById('productDetailDateRange');
        var headers = data.headers || [];
        var rows = data.rows || [];
        var productColIdx = headers.indexOf('产品归属');
        var unifiedIdColIdx = headers.indexOf('Unified ID');
        // 从哪进入都统一：先按产品名在 old 再在 new strategy 里解析 Unified ID 与 key（先精确再模糊）
        var resolvedUnifiedId = selectedProductForDetail.unifiedId ? String(selectedProductForDetail.unifiedId).trim() : '';
        var key = selectedProductForDetail.key || 'old';
        function resolveInStrategy(strategyRows, k) {
          if (!strategyRows || !strategyRows.length || unifiedIdColIdx < 0 || productColIdx < 0) return;
          for (var i = 0; i < strategyRows.length; i++) {
            var r = strategyRows[i];
            var cell = r[productColIdx];
            if (productNameExactMatch(selectedProductForDetail.name, cell)) {
              var uid = r[unifiedIdColIdx] != null ? String(r[unifiedIdColIdx]).trim() : '';
              if (uid) { resolvedUnifiedId = uid; selectedProductForDetail.unifiedId = uid; key = k; return; }
            }
          }
          for (var i = 0; i < strategyRows.length; i++) {
            var r = strategyRows[i];
            var cell = r[productColIdx];
            if (productNamesMatch(selectedProductForDetail.name, cell, cell)) {
              var uid = r[unifiedIdColIdx] != null ? String(r[unifiedIdColIdx]).trim() : '';
              if (uid) { resolvedUnifiedId = uid; selectedProductForDetail.unifiedId = uid; key = k; return; }
            }
          }
        }
        if (!resolvedUnifiedId && data.dataOld && data.dataNew) {
          resolveInStrategy(data.dataOld.rows, 'old');
          if (!resolvedUnifiedId) resolveInStrategy(data.dataNew.rows, 'new');
        } else if (!resolvedUnifiedId && rows.length && unifiedIdColIdx >= 0 && productColIdx >= 0) {
          for (var i = 0; i < rows.length; i++) {
            var r = rows[i];
            var cell = r[productColIdx];
            if (productNameExactMatch(selectedProductForDetail.name, cell)) {
              var uid = r[unifiedIdColIdx] != null ? String(r[unifiedIdColIdx]).trim() : '';
              if (uid) { resolvedUnifiedId = uid; selectedProductForDetail.unifiedId = uid; break; }
            }
          }
          if (!resolvedUnifiedId) {
            for (var i = 0; i < rows.length; i++) {
              var r = rows[i];
              var cell = r[productColIdx];
              if (productNamesMatch(selectedProductForDetail.name, cell, cell)) {
                var uid = r[unifiedIdColIdx] != null ? String(r[unifiedIdColIdx]).trim() : '';
                if (uid) { resolvedUnifiedId = uid; selectedProductForDetail.unifiedId = uid; break; }
              }
            }
          }
        }
        // 从公司维度/其他时间段进入时，该周可能无 strategy 文件；用同源的 _formatted 按产品名解析 Unified ID
        if (!resolvedUnifiedId && formattedData.rows && formattedData.rows.length && formattedData.headers) {
          var fmtProductCol = formattedData.headers.indexOf('产品归属');
          var fmtUnifiedCol = formattedData.headers.indexOf('Unified ID');
          if (fmtProductCol >= 0 && fmtUnifiedCol >= 0) {
            for (var fi = 0; fi < formattedData.rows.length; fi++) {
              var fr = formattedData.rows[fi];
              var fc = fr[fmtProductCol] != null ? String(fr[fmtProductCol]).trim() : '';
              if (productNameExactMatch(selectedProductForDetail.name, fc)) {
                var uid = fr[fmtUnifiedCol] != null ? String(fr[fmtUnifiedCol]).trim() : '';
                if (uid) { resolvedUnifiedId = uid; selectedProductForDetail.unifiedId = uid; break; }
              }
            }
            if (!resolvedUnifiedId) {
              for (var fi = 0; fi < formattedData.rows.length; fi++) {
                var fr = formattedData.rows[fi];
                var fc = fr[fmtProductCol] != null ? String(fr[fmtProductCol]).trim() : '';
                if (productNamesMatch(selectedProductForDetail.name, fc, fc)) {
                  var uid = fr[fmtUnifiedCol] != null ? String(fr[fmtUnifiedCol]).trim() : '';
                  if (uid) { resolvedUnifiedId = uid; selectedProductForDetail.unifiedId = uid; break; }
                }
              }
            }
          }
        }
        var row = null;
        for (var i = 0; i < rows.length; i++) {
          var r = rows[i];
          if (resolvedUnifiedId && unifiedIdColIdx >= 0 && r[unifiedIdColIdx] != null && String(r[unifiedIdColIdx]).trim() === resolvedUnifiedId) {
            row = r;
            break;
          }
          var cell = productColIdx >= 0 ? r[productColIdx] : '';
          if (productNamesMatch(selectedProductForDetail.name, cell, cell)) {
            row = r;
            break;
          }
        }
        var getVal = function (colName) {
          var idx = headers.indexOf(colName);
          if (idx < 0 || !row) return null;
          var v = row[idx];
          return v != null && v !== '' ? v : null;
        };
        var launchFromTotal = null;
        var companyFromFormatted = null;
        var companyFromMetrics = null;
        var allTimeDownloadsFromTotal = null;
        var allTimeRevenueFromTotal = null;
        // 产品累计安装/流水：从 metrics_total（最新一周有则用，没有则用上一周）按 Unified ID 匹配；无 Unified ID 时用上一周 metrics 按产品名匹配
        // 上线时间、公司归属：若当前周 formatted/strategy 无，则从 metrics_total 同条匹配行取（与累计数据同源，不依赖当前周）
        if (metricsData && metricsData.rows && metricsData.rows.length && metricsData.headers) {
          var mHeaders = metricsData.headers;
          var mRows = metricsData.rows;
          var mUnifiedCol = mHeaders.indexOf('Unified ID');
          var mProductCol = mHeaders.indexOf('产品归属');
          var mCompanyCol = mHeaders.indexOf('公司归属');
          var mAllTimeDownloadsCol = mHeaders.indexOf('All Time Downloads (WW)');
          var mAllTimeRevenueCol = mHeaders.indexOf('All Time Revenue (WW)');
          var mLaunchCol = mHeaders.indexOf('第三方记录最早上线时间') >= 0 ? mHeaders.indexOf('第三方记录最早上线时间') : mHeaders.indexOf('Earliest Release Date');
          if (mUnifiedCol >= 0 && (mAllTimeDownloadsCol >= 0 || mAllTimeRevenueCol >= 0)) {
            var found = false;
            if (resolvedUnifiedId) {
              for (var k = 0; k < mRows.length; k++) {
                var mr = mRows[k];
                var rowId = mr[mUnifiedCol] != null ? String(mr[mUnifiedCol]).trim() : '';
                if (rowId === resolvedUnifiedId) {
                  if (mAllTimeDownloadsCol >= 0 && mr[mAllTimeDownloadsCol] != null && mr[mAllTimeDownloadsCol] !== '') allTimeDownloadsFromTotal = mr[mAllTimeDownloadsCol];
                  if (mAllTimeRevenueCol >= 0 && mr[mAllTimeRevenueCol] != null && mr[mAllTimeRevenueCol] !== '') allTimeRevenueFromTotal = mr[mAllTimeRevenueCol];
                  if (launchFromTotal == null && mLaunchCol >= 0 && mr[mLaunchCol] != null && mr[mLaunchCol] !== '') launchFromTotal = mr[mLaunchCol];
                  if (mCompanyCol >= 0 && mr[mCompanyCol] != null && String(mr[mCompanyCol]).trim() !== '' && String(mr[mCompanyCol]).trim().indexOf('汇总') === -1) companyFromMetrics = String(mr[mCompanyCol]).trim();
                  found = true;
                  break;
                }
              }
            }
            if (!found && mProductCol >= 0) {
              for (var k = 0; k < mRows.length; k++) {
                var mr = mRows[k];
                var cell = mr[mProductCol];
                if (productNameExactMatch(selectedProductForDetail.name, cell)) {
                  if (mAllTimeDownloadsCol >= 0 && mr[mAllTimeDownloadsCol] != null && mr[mAllTimeDownloadsCol] !== '') allTimeDownloadsFromTotal = mr[mAllTimeDownloadsCol];
                  if (mAllTimeRevenueCol >= 0 && mr[mAllTimeRevenueCol] != null && mr[mAllTimeRevenueCol] !== '') allTimeRevenueFromTotal = mr[mAllTimeRevenueCol];
                  if (launchFromTotal == null && mLaunchCol >= 0 && mr[mLaunchCol] != null && mr[mLaunchCol] !== '') launchFromTotal = mr[mLaunchCol];
                  if (mCompanyCol >= 0 && mr[mCompanyCol] != null && String(mr[mCompanyCol]).trim() !== '' && String(mr[mCompanyCol]).trim().indexOf('汇总') === -1) companyFromMetrics = String(mr[mCompanyCol]).trim();
                  if (mUnifiedCol >= 0 && mr[mUnifiedCol] != null) selectedProductForDetail.unifiedId = String(mr[mUnifiedCol]).trim();
                  break;
                }
              }
            }
          }
        }
        // formattedData：取上线时间；累计安装/流水仅来自 metrics_total（formatted 无 All Time 列则不回退）
        if (formattedData.rows && formattedData.rows.length && formattedData.headers) {
          var totalHeaders = formattedData.headers;
          var totalRows = formattedData.rows;
          var totalProductCol = totalHeaders.indexOf('产品归属');
          var totalCompanyCol = totalHeaders.indexOf('公司归属');
          var totalUnifiedCol = totalHeaders.indexOf('Unified ID');
          var totalLaunchCol = totalHeaders.indexOf('第三方记录最早上线时间');
          var totalAllTimeDownloadsCol = totalHeaders.indexOf('All Time Downloads (WW)');
          var totalAllTimeRevenueCol = totalHeaders.indexOf('All Time Revenue (WW)');
          for (var j = 0; j < totalRows.length; j++) {
            var tr = totalRows[j];
            var match = false;
            if (resolvedUnifiedId && totalUnifiedCol >= 0) {
              var rowIdFormatted = tr[totalUnifiedCol] != null ? String(tr[totalUnifiedCol]).trim() : '';
              if (rowIdFormatted === resolvedUnifiedId) match = true;
            }
            if (!match && !resolvedUnifiedId && totalProductCol >= 0) {
              var tc = tr[totalProductCol];
              if (productNamesMatch(selectedProductForDetail.name, tc, tc)) match = true;
            }
            if (match) {
              if (isSummaryRow(tr)) continue;
              if (totalCompanyCol >= 0 && tr[totalCompanyCol] != null && String(tr[totalCompanyCol]).trim() !== '' && String(tr[totalCompanyCol]).trim().indexOf('汇总') === -1) companyFromFormatted = String(tr[totalCompanyCol]).trim();
              if (totalLaunchCol >= 0 && tr[totalLaunchCol] != null && tr[totalLaunchCol] !== '') launchFromTotal = tr[totalLaunchCol];
              if (allTimeDownloadsFromTotal == null && totalAllTimeDownloadsCol >= 0 && tr[totalAllTimeDownloadsCol] != null && tr[totalAllTimeDownloadsCol] !== '') allTimeDownloadsFromTotal = tr[totalAllTimeDownloadsCol];
              if (allTimeRevenueFromTotal == null && totalAllTimeRevenueCol >= 0 && tr[totalAllTimeRevenueCol] != null && tr[totalAllTimeRevenueCol] !== '') allTimeRevenueFromTotal = tr[totalAllTimeRevenueCol];
              break;
            }
          }
        }
        var companyEl = document.getElementById('productDetailCompany');
        var newOldEl = document.getElementById('productDetailNewOld');
        var launchEl = document.getElementById('productDetailLaunch');
        var themeEl = document.getElementById('productDetailTheme');
        var styleEl = document.getElementById('productDetailStyle');
        var installEl = document.getElementById('productDetailInstall');
        var rankInstallEl = document.getElementById('productDetailRankInstall');
        var revenueEl = document.getElementById('productDetailRevenue');
        var rankRevenueEl = document.getElementById('productDetailRankRevenue');
        if (companyEl) companyEl.textContent = (getVal('公司归属') != null && String(getVal('公司归属')).indexOf('汇总') === -1 ? getVal('公司归属') : (companyFromFormatted != null ? companyFromFormatted : (companyFromMetrics != null ? companyFromMetrics : '—')));
        if (newOldEl) newOldEl.textContent = key === 'old' ? '旧产品' : '新产品';
        if (launchEl) {
          var t = launchFromTotal != null && launchFromTotal !== '' ? launchFromTotal : getVal('第三方记录最早上线时间');
          launchEl.textContent = t != null && t !== '' ? String(t).replace(/\s+\d{2}:\d{2}:\d{2}$/, '') : '—';
        }
        // 题材/画风：统一从固定表 product_theme_style_mapping.json 取，先按 Unified ID 再按产品名（从哪进入都一致）
        var mappingEntry = null;
        if (productThemeStyleMapping && resolvedUnifiedId && productThemeStyleMapping.byUnifiedId && productThemeStyleMapping.byUnifiedId[resolvedUnifiedId]) {
          mappingEntry = productThemeStyleMapping.byUnifiedId[resolvedUnifiedId];
        }
        if (!mappingEntry && productThemeStyleMapping && productThemeStyleMapping.byProductName) {
          var pname = selectedProductForDetail.name ? String(selectedProductForDetail.name).trim() : '';
          if (pname && productThemeStyleMapping.byProductName[pname]) mappingEntry = productThemeStyleMapping.byProductName[pname];
          if (!mappingEntry) {
            for (var pk in productThemeStyleMapping.byProductName) {
              if (productNameExactMatch(selectedProductForDetail.name, pk)) { mappingEntry = productThemeStyleMapping.byProductName[pk]; break; }
            }
          }
          if (!mappingEntry) {
            for (var pk in productThemeStyleMapping.byProductName) {
              if (productNamesMatch(selectedProductForDetail.name, pk, pk)) { mappingEntry = productThemeStyleMapping.byProductName[pk]; break; }
            }
          }
        }
        if (themeEl) themeEl.textContent = (mappingEntry && mappingEntry['题材']) ? mappingEntry['题材'] : (getVal('题材标签') != null ? getVal('题材标签') : '—');
        if (styleEl) styleEl.textContent = (mappingEntry && mappingEntry['画风']) ? mappingEntry['画风'] : (getVal('画风标签') != null ? getVal('画风标签') : '—');
        if (dateEl) dateEl.textContent = weekTagToSlashDateRange(currentYear, currentWeek);
        if (installEl) {
          var installVal = allTimeDownloadsFromTotal;
          if (installVal != null && installVal !== '') {
            var n = typeof installVal === 'number' ? installVal : (parseFloat(String(installVal).replace(/,/g, '')) || parseInt(String(installVal).replace(/,/g, ''), 10));
            installEl.textContent = !Number.isNaN(n) ? n.toLocaleString('en-US') : '—';
          } else installEl.textContent = '—';
        }
        // 产品赛道排名：仅在【产品归属】非空的项中排名（与累计数据统一，同一份 metrics_total）
        var rankInstall = 0;
        var rankRevenue = 0;
        if (metricsData && metricsData.rows && metricsData.rows.length && metricsData.headers) {
          var mH = metricsData.headers;
          var mR = metricsData.rows;
          var mU = mH.indexOf('Unified ID');
          var mP = mH.indexOf('产品归属');
          var mD = mH.indexOf('All Time Downloads (WW)');
          var mRev = mH.indexOf('All Time Revenue (WW)');
          if (mP >= 0 && (mD >= 0 || mRev >= 0)) {
            var list = [];
            var seenKey = {};
            for (var qi = 0; qi < mR.length; qi++) {
              var qr = mR[qi];
              var pn = mP >= 0 && qr[mP] != null ? String(qr[mP]).trim() : '';
              if (!pn) continue;
              var uid = mU >= 0 && qr[mU] != null ? String(qr[mU]).trim() : '';
              var key = uid || pn;
              if (seenKey[key]) continue;
              seenKey[key] = true;
              var dVal = mD >= 0 && qr[mD] != null && qr[mD] !== '' ? (typeof qr[mD] === 'number' ? qr[mD] : (parseFloat(String(qr[mD]).replace(/,/g, '')) || parseInt(String(qr[mD]).replace(/,/g, ''), 10))) : 0;
              var rVal = mRev >= 0 && qr[mRev] != null && qr[mRev] !== '' ? (typeof qr[mRev] === 'number' ? qr[mRev] : (parseFloat(String(qr[mRev]).replace(/[$,]/g, '')) || parseInt(String(qr[mRev]).replace(/[$,]/g, ''), 10))) : 0;
              if (Number.isNaN(dVal)) dVal = 0;
              if (Number.isNaN(rVal)) rVal = 0;
              list.push({ uid: uid, pn: pn, d: dVal, r: rVal });
            }
            list.sort(function (a, b) { return b.d - a.d; });
            for (var ri = 0; ri < list.length; ri++) {
              var it = list[ri];
              if ((resolvedUnifiedId && it.uid === resolvedUnifiedId) || (!resolvedUnifiedId && selectedProductForDetail.name && productNameExactMatch(selectedProductForDetail.name, it.pn))) {
                rankInstall = ri + 1;
                break;
              }
            }
            list.sort(function (a, b) { return b.r - a.r; });
            for (var rj = 0; rj < list.length; rj++) {
              var jt = list[rj];
              if ((resolvedUnifiedId && jt.uid === resolvedUnifiedId) || (!resolvedUnifiedId && selectedProductForDetail.name && productNameExactMatch(selectedProductForDetail.name, jt.pn))) {
                rankRevenue = rj + 1;
                break;
              }
            }
          }
        }
        if (rankInstallEl) rankInstallEl.textContent = rankInstall > 0 ? '第' + rankInstall + '名' : '—';
        if (revenueEl) {
          var revenueVal = allTimeRevenueFromTotal;
          if (revenueVal != null && revenueVal !== '') {
            var revenueStr = String(revenueVal).replace(/[$,]/g, '');
            var rn = typeof revenueVal === 'number' ? revenueVal : (parseFloat(revenueStr) || parseInt(revenueStr, 10));
            revenueEl.textContent = !Number.isNaN(rn) ? '$' + rn.toLocaleString('en-US', { minimumFractionDigits: 0, maximumFractionDigits: 0 }) : '—';
          } else revenueEl.textContent = '—';
        }
        if (rankRevenueEl) rankRevenueEl.textContent = rankRevenue > 0 ? '第' + rankRevenue + '名' : '—';
  }

  function weekSortKey(year, weekTag) {
    var y = parseInt(year, 10) || 0;
    var s = String(weekTag || '').trim();
    var m = s.length >= 4 ? parseInt(s.slice(0, 2), 10) || 0 : 0;
    var d = s.length >= 4 ? parseInt(s.slice(2, 4), 10) || 0 : 0;
    return y * 10000 + m * 100 + d;
  }

  function destroyProductDetailCharts() {
    if (productDetailLineChartInstance) {
      productDetailLineChartInstance.destroy();
      productDetailLineChartInstance = null;
    }
    if (productDetailStackedBarChartInstance) {
      productDetailStackedBarChartInstance.destroy();
      productDetailStackedBarChartInstance = null;
    }
  }

  function loadProductTrendCharts() {
    if (!selectedProductForDetail || !window.Chart) return;
    var loadingEl = document.getElementById('productDetailTrendLoading');
    if (loadingEl) loadingEl.style.display = '';
    destroyProductDetailCharts();
    fetch(base + '/data/weeks_index.json')
      .then(function (r) { if (!r.ok) throw new Error(r.statusText); return r.json(); })
      .then(function (index) {
        var pairs = [];
        Object.keys(index || {}).filter(function (y) { return /^\d{4}$/.test(y); }).forEach(function (year) {
          (index[year] || []).forEach(function (weekTag) {
            pairs.push({ year: year, weekTag: weekTag });
          });
        });
        pairs.sort(function (a, b) {
          return weekSortKey(a.year, a.weekTag) - weekSortKey(b.year, b.weekTag);
        });
        var weekLabels = [];
        var installs = [];
        var asiaT1 = [];
        var euT1 = [];
        var t2 = [];
        var t3 = [];
        function findRowInStrategy(headers, rows) {
          var productColIdx = headers.indexOf('产品归属');
          var unifiedIdColIdx = headers.indexOf('Unified ID');
          for (var j = 0; j < rows.length; j++) {
            var r = rows[j];
            if (selectedProductForDetail.unifiedId && unifiedIdColIdx >= 0 && r[unifiedIdColIdx] != null && String(r[unifiedIdColIdx]).trim() === selectedProductForDetail.unifiedId) return r;
            var cell = productColIdx >= 0 ? r[productColIdx] : '';
            if (productNamesMatch(selectedProductForDetail.name, cell, cell)) return r;
          }
          return null;
        }
        function fetchNext(i) {
          if (i >= pairs.length) {
            if (loadingEl) loadingEl.style.display = 'none';
            renderProductDetailTrendCharts(weekLabels, installs, asiaT1, euT1, t2, t3);
            return;
          }
          var p = pairs[i];
          weekLabels.push(p.weekTag);
          var urlTotal = base + '/data/' + p.year + '/' + p.weekTag + '_formatted.json';
          var urlStrategyOld = base + '/data/' + p.year + '/' + p.weekTag + '/product_strategy_old.json';
          var urlStrategyNew = base + '/data/' + p.year + '/' + p.weekTag + '/product_strategy_new.json';
          Promise.all([
            fetch(urlTotal).then(function (r) { return r.ok ? r.json() : { headers: [], rows: [] }; }),
            fetch(urlStrategyOld).then(function (r) { return r.ok ? r.json() : { headers: [], rows: [] }; }),
            fetch(urlStrategyNew).then(function (r) { return r.ok ? r.json() : { headers: [], rows: [] }; })
          ]).then(function (results) {
            var totalData = results[0];
            var strategyOld = results[1];
            var strategyNew = results[2];
            var installVal = 0;
            var headersTotal = totalData.headers || [];
            var rowsTotal = totalData.rows || [];
            var productColTotal = headersTotal.indexOf('产品归属');
            var idxInstallTotal = headersTotal.indexOf('当周周安装');
            if (productColTotal >= 0) {
              var selectedNorm = normalizeProductName(selectedProductForDetail.name || '');
              for (var t = 0; t < rowsTotal.length; t++) {
                var cell = rowsTotal[t][productColTotal];
                var cellNorm = normalizeProductName(cell);
                if (selectedNorm && cellNorm === selectedNorm) {
                  if (idxInstallTotal >= 0) {
                    var v = rowsTotal[t][idxInstallTotal];
                    installVal = typeof v === 'number' ? v : (parseFloat(v) || parseInt(v, 10) || 0);
                  }
                  break;
                }
              }
            }
            installs.push(installVal);
            var row = findRowInStrategy(strategyOld.headers || [], strategyOld.rows || []);
            var headers = strategyOld.headers || [];
            if (!row) {
              row = findRowInStrategy(strategyNew.headers || [], strategyNew.rows || []);
              headers = strategyNew.headers || [];
            }
            var idxAsia = headers.indexOf('亚洲 T1 市场获量');
            var idxEu = headers.indexOf('欧美 T1 市场获量');
            var idxT2 = headers.indexOf('T2 市场获量');
            var idxT3 = headers.indexOf('T3 市场获量');
            if (row && idxAsia >= 0 && idxEu >= 0 && idxT2 >= 0 && idxT3 >= 0) {
              asiaT1.push(typeof row[idxAsia] === 'number' ? row[idxAsia] : (parseFloat(row[idxAsia]) || 0));
              euT1.push(typeof row[idxEu] === 'number' ? row[idxEu] : (parseFloat(row[idxEu]) || 0));
              t2.push(typeof row[idxT2] === 'number' ? row[idxT2] : (parseFloat(row[idxT2]) || 0));
              t3.push(typeof row[idxT3] === 'number' ? row[idxT3] : (parseFloat(row[idxT3]) || 0));
            } else {
              asiaT1.push(0);
              euT1.push(0);
              t2.push(0);
              t3.push(0);
            }
            fetchNext(i + 1);
          }).catch(function () {
            installs.push(0);
            asiaT1.push(0);
            euT1.push(0);
            t2.push(0);
            t3.push(0);
            fetchNext(i + 1);
          });
        }
        fetchNext(0);
      })
      .catch(function () {
        if (loadingEl) loadingEl.style.display = 'none';
      });
  }

  function renderProductDetailTrendCharts(weekLabels, installs, asiaT1, euT1, t2, t3) {
    if (!window.Chart) return;
    destroyProductDetailCharts();
    var lineCtx = document.getElementById('productDetailLineChart');
    var barCtx = document.getElementById('productDetailStackedBarChart');
    if (!lineCtx || !barCtx) return;
    productDetailLineChartInstance = new Chart(lineCtx, {
      type: 'line',
      data: {
        labels: weekLabels,
        datasets: [{ label: '当周周安装', data: installs, borderColor: '#3498db', backgroundColor: 'rgba(52,152,219,0.1)', fill: true, tension: 0.2 }]
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        plugins: { legend: { display: false } },
        scales: {
          x: { display: true, title: { display: false } },
          y: { beginAtZero: true, title: { display: true, text: '安装量' } }
        }
      }
    });
    productDetailStackedBarChartInstance = new Chart(barCtx, {
      type: 'bar',
      data: {
        labels: weekLabels,
        datasets: [
          { label: '亚洲 T1', data: asiaT1, backgroundColor: 'rgba(52,152,219,0.8)', stack: 'stack1' },
          { label: '欧美 T1', data: euT1, backgroundColor: 'rgba(46,204,113,0.8)', stack: 'stack1' },
          { label: 'T2', data: t2, backgroundColor: 'rgba(241,196,15,0.8)', stack: 'stack1' },
          { label: 'T3', data: t3, backgroundColor: 'rgba(230,126,34,0.8)', stack: 'stack1' }
        ]
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        plugins: { legend: { position: 'top' } },
        scales: {
          x: { stacked: true, title: { display: false } },
          y: { stacked: true, beginAtZero: true, title: { display: true, text: '获量' } }
        }
      }
    });
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
    if (!sidebarYearsEl) {
      setState('empty');
      return;
    }
    if (index && index.data_range) {
      dataRange = index.data_range;
    } else {
      dataRange = null;
    }
    var dataTimeRangeEl = document.getElementById('dataTimeRange');
    if (dataTimeRangeEl) dataTimeRangeEl.textContent = dataRange ? '数据时间: ' + formatDataRangeDisplay(dataRange) : '数据时间: —';
    weeksIndex = index;
    const years = Object.keys(index || {}).filter(y => /^\d{4}$/.test(y)).sort((a, b) => Number(b) - Number(a));
    // 打开前端时默认选最新时间段：所有 (年,周) 按 weekSortKey 降序取第一个
    var allPairs = [];
    years.forEach(function (y) {
      (index[y] || []).forEach(function (wt) { allPairs.push({ year: y, weekTag: wt }); });
    });
    allPairs.sort(function (a, b) { return weekSortKey(b.year, b.weekTag) - weekSortKey(a.year, a.weekTag); });
    if (allPairs.length) {
      currentYear = allPairs[0].year;
      currentWeek = allPairs[0].weekTag;
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
        // 每月内按时间倒序：最新周在最上，次新在下
        byMonth[m].slice().reverse().forEach(weekTag => {
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

    if (years.length && allPairs.length) {
      currentYear = allPairs[0].year;
      currentWeek = allPairs[0].weekTag;
      // 每次打开前端自动跳转到最新时间段的公司维度大盘数据首页
      currentDimension = 'company';
      if (window.location.hash !== '#company') window.location.hash = '#company';
      switchDimension('company');  // 内部会 loadCompanyWeek(currentYear, currentWeek)
    } else {
      setState('empty');
      switchDimension(currentDimension);
    }
  }

  function loadWeeksIndex() {
    setState('loading');
    // 优先使用 start_server 注入的周索引（访问 /frontend/ 时服务器会注入），不依赖 fetch
    var dataBase = (document.location.pathname || '').indexOf('frontend') >= 0 ? '/frontend' : base;
    if (typeof window.__WEEKS_INDEX__ !== 'undefined' && window.__WEEKS_INDEX__ != null) {
      try {
        buildSidebar(window.__WEEKS_INDEX__);
        if (typeof window.__PRODUCT_THEME_STYLE_MAPPING__ !== 'undefined' && window.__PRODUCT_THEME_STYLE_MAPPING__ != null) productThemeStyleMapping = window.__PRODUCT_THEME_STYLE_MAPPING__;
        else fetch(dataBase + '/data/product_theme_style_mapping.json').then(function (r) { return r.ok ? r.json() : null; }).catch(function () { return null; }).then(function (m) { productThemeStyleMapping = m && (m.byUnifiedId || m.byProductName) ? m : { byUnifiedId: {}, byProductName: {} }; });
        return;
      } catch (e) {
        if (sidebarYearsEl) sidebarYearsEl.innerHTML = '<p class="empty">加载周索引出错</p>';
        setState('empty');
        console.warn('buildSidebar error:', e);
        return;
      }
    }
    // 未注入时再请求 /frontend/data/weeks_index.json
    if (typeof dataBase === 'undefined') dataBase = (document.location.pathname || '').indexOf('frontend') >= 0 ? '/frontend' : base;
    var dataUrl = dataBase + '/data/weeks_index.json';
    var controller = typeof AbortController !== 'undefined' ? new AbortController() : null;
    var timeoutId = controller ? window.setTimeout(function () {
      controller.abort();
    }, 10000) : null;
    var opts = controller ? { signal: controller.signal } : {};
    fetch(dataUrl, opts)
      .then(function (r) {
        if (timeoutId) clearTimeout(timeoutId);
        if (!r.ok) throw new Error(r.status + ' ' + r.statusText);
        return r.json();
      })
      .then(function (data) {
        if (timeoutId) clearTimeout(timeoutId);
        try {
          buildSidebar(data);
          if (typeof window.__PRODUCT_THEME_STYLE_MAPPING__ !== 'undefined' && window.__PRODUCT_THEME_STYLE_MAPPING__ != null) productThemeStyleMapping = window.__PRODUCT_THEME_STYLE_MAPPING__;
          else fetch(dataBase + '/data/product_theme_style_mapping.json').then(function (r) { return r.ok ? r.json() : null; }).catch(function () { return null; }).then(function (m) { productThemeStyleMapping = m && (m.byUnifiedId || m.byProductName) ? m : { byUnifiedId: {}, byProductName: {} }; });
        } catch (e) {
          if (sidebarYearsEl) sidebarYearsEl.innerHTML = '<p class="empty">加载周索引出错</p>';
          setState('empty');
          console.warn('buildSidebar error:', e);
        }
      })
      .catch(function (err) {
        if (timeoutId) clearTimeout(timeoutId);
        if (sidebarYearsEl) sidebarYearsEl.innerHTML = '<p class="empty">无周期数据（请确认已启动 start_server.py 并访问 http://localhost:端口/frontend/）</p>';
        setState('empty');
        console.warn('loadWeeksIndex failed:', err);
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

  if (searchInput) searchInput.addEventListener('input', filterRows);
  if (productSearchInput) productSearchInput.addEventListener('input', filterRows);
  if (btnDownload) btnDownload.addEventListener('click', downloadTable);
  if (btnProductDownload) btnProductDownload.addEventListener('click', downloadTable);

  if (productSelect) productSelect.addEventListener('change', function () {
    if (currentDimension === 'product' && currentYear && currentWeek) loadProductWeek(currentYear, currentWeek);
  });

  if (creativeProductTable) creativeProductTable.addEventListener('change', function () {
    if (currentDimension !== 'creative' || !creativeProductsIndex) return;
    fillCreativeProductSelect();
    if (creativeProductSelect) creativeProductSelect.value = '';
    if (creativeProductDataRow) creativeProductDataRow.style.display = 'none';
    creativeRows = null;
    creativeFilteredRows = null;
    renderCreativeTableIfNeeded();
  });

  if (creativeProductSelect) creativeProductSelect.addEventListener('change', function () {
    if (currentDimension !== 'creative') return;
    if (this.value) loadCreativeProductAds();
    else {
      creativeRows = null;
      creativeFilteredRows = null;
      if (creativeProductDataRow) creativeProductDataRow.style.display = 'none';
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

  if (btnCreativeDownload) btnCreativeDownload.addEventListener('click', function () {
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

  if (tableBody) tableBody.addEventListener('click', function (e) {
    // e.target 可能是 <a> 内的文本节点，文本节点无 .closest，需从父元素查找
    var start = e.target && e.target.nodeType === 1 ? e.target : (e.target && e.target.parentElement);
    var companyLink = start && start.closest ? start.closest('a.company-link') : null;
    if (companyLink) {
      e.preventDefault();
      selectedCompanyForDetail = companyLink.textContent.trim();
      switchDimension('company-detail');
      return;
    }
    var link = start && start.closest ? start.closest('a.product-link') : null;
    if (link) {
      e.preventDefault();
      var productName = link.textContent.trim();
      var tr = start && start.closest ? start.closest('tr') : null;
      var unifiedId = (tr && tr.dataset && tr.dataset.unifiedId) ? tr.dataset.unifiedId : null;
      if (currentDimension === 'product') {
        productDetailOrigin = 'product';
        selectedProductForDetail = { name: productName, key: productSelect.value || 'old', unifiedId: unifiedId };
        switchDimension('product-detail');
      } else if (currentDimension === 'company') {
        productDetailOrigin = 'company';
        selectedProductForDetail = { name: productName, key: 'old', unifiedId: unifiedId };
        switchDimension('product-detail');
      } else {
        pendingCreativeProduct = productName;
        switchDimension('creative');
      }
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

  // 浏览器前进/后退时根据 URL hash 同步当前维度与面板，否则返回上一页时内容不会切换
  window.addEventListener('hashchange', function () {
    var h = (window.location.hash || '#company').replace('#', '').toLowerCase();
    if (['company', 'company-detail', 'product', 'product-detail', 'creative', 'combo'].indexOf(h) >= 0 && currentDimension !== h) {
      currentDimension = h;
      switchDimension(h);
    }
  });

  var companyDetailSubNavEl = document.getElementById('companyDetailSubNav');
  if (companyDetailSubNavEl) {
    companyDetailSubNavEl.querySelectorAll('.company-detail-sub-link').forEach(function (link) {
      link.addEventListener('click', function (e) {
        e.preventDefault();
        var tab = this.dataset.tab;
        companyDetailSubNavEl.querySelectorAll('.company-detail-sub-link').forEach(function (l) { l.classList.remove('active'); });
        this.classList.add('active');
        if (tab === 'market') switchDimension('company');
      });
    });
  }

  document.querySelectorAll('.product-detail-sub-link').forEach(function (link) {
    link.addEventListener('click', function (e) {
      e.preventDefault();
      var tab = this.dataset.tab;
      document.querySelectorAll('.product-detail-sub-link').forEach(function (l) { l.classList.remove('active'); });
      this.classList.add('active');
      if (tab === 'market') switchDimension(productDetailOrigin || 'product');
      // detail 保持当前产品详细看板
    });
  });

  var btnProductDetailToCreative = document.getElementById('btnProductDetailToCreative');
  if (btnProductDetailToCreative) {
    btnProductDetailToCreative.addEventListener('click', function () {
      if (selectedProductForDetail && selectedProductForDetail.name) {
        pendingCreativeProduct = selectedProductForDetail.name;
        if (creativeProductTable) creativeProductTable.value = 'strategy_' + (selectedProductForDetail.key || 'old');
        switchDimension('creative');
      }
    });
  }

  var companyHomeSubNav = document.getElementById('companyHomeSubNav');
  if (companyHomeSubNav) {
    companyHomeSubNav.querySelectorAll('.company-detail-sub-link').forEach(function (link) {
      link.addEventListener('click', function (e) {
        e.preventDefault();
        var tab = this.dataset.tab || 'overall';
        if (tab !== 'overall' && tab !== 'detail') return;
        currentCompanySubTab = tab;
        companyHomeSubNav.querySelectorAll('.company-detail-sub-link').forEach(function (l) { l.classList.remove('active'); });
        this.classList.add('active');
        if (currentDimension === 'company') showCompanySubTabContent();
      });
    });
  }

  setInterval(updateLocalTime, 1000);
  if (localTimeEl) updateLocalTime();

  // 等 DOM 完全就绪后再请求周索引，避免个别环境下取不到元素或请求路径异常
  function init() {
    loadWeeksIndex();
  }
  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init);
  } else {
    init();
  }
})();
