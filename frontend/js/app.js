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
  const panelMaintenance = document.getElementById('panelMaintenance');
  const panelApproval = document.getElementById('panelApproval');
  const panelAdvancedQuery = document.getElementById('panelAdvancedQuery');
  const sidebarApprovalTabs = document.getElementById('sidebarApprovalTabs');
  const sidebarAdvancedTables = document.getElementById('sidebarAdvancedTables');
  const advancedQueryTableList = document.getElementById('advancedQueryTableList');
  var currentApprovalTab = 'pending';  // 'pending' | 'existing'
  const advancedQuerySql = document.getElementById('advancedQuerySql');
  const advancedQueryRunBtn = document.getElementById('advancedQueryRunBtn');
  const advancedQueryDownloadBtn = document.getElementById('advancedQueryDownloadBtn');
  const advancedQueryStatus = document.getElementById('advancedQueryStatus');
  const advancedQueryResultWrap = document.getElementById('advancedQueryResultWrap');
  const advancedQueryResultHead = document.getElementById('advancedQueryResultHead');
  const advancedQueryResultBody = document.getElementById('advancedQueryResultBody');
  const advancedQueryEmpty = document.getElementById('advancedQueryEmpty');
  const comboTitleEl = document.getElementById('comboTitle');
  const comboPeriodEl = document.getElementById('comboPeriod');
  const tableWrap = document.getElementById('tableWrap');
  const productTitleEl = document.getElementById('productTitle');
  const productPeriodEl = document.getElementById('productPeriod');
  const productSearchInput = document.getElementById('productSearchInput');
  const productSelect = document.getElementById('productSelect');
  const productToolbar = document.getElementById('productToolbar');
  const productNewGameWrap = document.getElementById('productNewGameWrap');
  const productNewGameEmpty = document.getElementById('productNewGameEmpty');
  const productNewGameTableWrap = document.getElementById('productNewGameTableWrap');
  const productNewGameTableHead = document.getElementById('productNewGameTableHead');
  const productNewGameTableBody = document.getElementById('productNewGameTableBody');
  const productNewGameSearch = document.getElementById('productNewGameSearch');
  const productHomeDetailPlaceholder = document.getElementById('productHomeDetailPlaceholder');
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
  const btnDownloadMonitorTable = document.getElementById('btnDownloadMonitorTable');
  const btnProductDownload = document.getElementById('btnProductDownload');
  const btnCreativeDownload = document.getElementById('btnCreativeDownload');
  const panelBasetable = document.getElementById('panelBasetable');
  const basetableSubNav = document.getElementById('basetableSubNav');
  const basetableTitle = document.getElementById('basetableTitle');
  const basetablePeriodHint = document.getElementById('basetablePeriodHint');
  const basetablePeriodText = document.getElementById('basetablePeriodText');
  const basetableEmpty = document.getElementById('basetableEmpty');
  const basetableTableWrap = document.getElementById('basetableTableWrap');
  const basetableTableHead = document.getElementById('basetableTableHead');
  const basetableTableBody = document.getElementById('basetableTableBody');
  const basetableMetricsToolbar = document.getElementById('basetableMetricsToolbar');
  const basetableMetricsSearch = document.getElementById('basetableMetricsSearch');
  const basetableMetricsHint = document.getElementById('basetableMetricsHint');
  const basetableOtherToolbar = document.getElementById('basetableOtherToolbar');
  const basetableOtherSearch = document.getElementById('basetableOtherSearch');
  const basetableOtherHint = document.getElementById('basetableOtherHint');
  const tableHead = document.getElementById('tableHead');
  const tableBody = document.getElementById('tableBody');
  const searchInput = document.getElementById('searchInput');
  const loadingEl = document.getElementById('loading');
  const emptyEl = document.getElementById('empty');
  const errorEl = document.getElementById('error');
  const loginOverlay = document.getElementById('loginOverlay');
  const appWrap = document.getElementById('appWrap');
  const loginForm = document.getElementById('loginForm');
  const loginUsername = document.getElementById('loginUsername');
  const loginPassword = document.getElementById('loginPassword');
  const loginError = document.getElementById('loginError');
  const loginSubmit = document.getElementById('loginSubmit');
  const btnLogout = document.getElementById('btnLogout');
  const headerUsername = document.getElementById('headerUsername');

  const API_BASE = document.location.pathname.startsWith('/frontend') ? '' : '';
  /** 数据接口统一走 /api/data/*（后端从 MySQL 或文件返回） */
  const DATA_API_BASE = '/api/data';

  let weeksIndex = null;
  let productThemeStyleMapping = null;  // { byUnifiedId: { id: { 题材, 画风 } } }，来自 product_theme_style_mapping.json（mapping/产品归属.xlsx）
  let dataRange = null;  // { start: "YYYY-MM-DD", end: "YYYY-MM-DD" }，由 weeks_index.json 的 data_range 提供，跑完脚本后自动更新
  let currentYear = null;
  let currentWeek = null;
  let currentDimension = 'company';
  let currentCompanySubTab = 'overall';
  let currentProductSubTab = 'overall';  // 产品维度子导航：overall=大盘数据, new=上线新游, detail=详细数据
  let currentBasetableTab = null;  // 数据底表子导航：metrics_total|product_mapping|company_mapping|new_products|theme_label|gameplay_label|art_style_label
  let basetableCachedData = null;  // 非产品总表时缓存 { tab, headers, rows }，供搜索筛选
  const DETAIL_CACHE_MAX = 50;  // 公司/产品详情缓存条数上限，超出时删最旧
  let detailCache = { company: {}, product: {} };  // 详情页内存缓存，同页面重复进入时直接复用
  const VALID_DIMS = ['company', 'company-detail', 'product', 'product-detail', 'creative', 'combo', 'basetable', 'maintenance', 'approval', 'advanced_query'];
  let currentUserRole = '';  // 'super_admin' | 'user'，登录后由 /api/auth/check 设置
  (function () {
    var h = (window.location.hash || '#company').replace('#', '').toLowerCase();
    if (VALID_DIMS.indexOf(h) >= 0) currentDimension = h;
  })();
  let currentData = null;
  let filteredRows = null;
  let sortCol = -1;
  let sortAsc = true;
  let creativeProductsIndex = null;
  let creativeRows = null;
  let creativeFilteredRows = null;
  let productDataForCreative = null;
  let newProductsData = null;   // { headers: [], rows: [] }，来自 data/new_products.json（newproducts/*.xlsx 转出）
  let newProductsFilteredRows = null;
  let newProductsWeekFilteredRows = null;  // 按当前周（所属周）过滤后的行，搜索在此基础上再过滤
  let newProductsMetricsByWeek = null;    // [{ year, week, productNames: [], nameToUnifiedId: {} }, ...]，按周存总表产品名与 Unified ID，匹配时只考虑「包含或晚于开测日期」的周
  let newProductsInProductMapping = null; // Set<string> 产品归属表中存在的「产品归属」
  // 上线新游展示列：产品名、产品归属、发行商、公司归属(空填未知)、开测日期、是否下架、是否在总表中存在、是否在产品表中存在、操作
  const NEW_PRODUCTS_DISPLAY = ['产品名', '产品归属', '发行商', '公司归属', '开测日期', '是否下架', '是否在总表中存在', '是否在产品表中存在', '操作'];
  let pendingCreativeProduct = null;  // 从公司维度点击标黄行跳转素材维度时待选中的产品归属
  let selectedProductForDetail = null;  // 从产品维度点击产品进入产品详细看板时：{ name, key: 'old'|'new' }
  let productDetailOrigin = 'product';  // 进入产品详细看板时的来源：'company' 从公司大盘点入，'product' 从产品大盘点入；用于「大盘数据」按钮跳回对应维度
  let selectedCompanyForDetail = null;  // 从公司维度大盘数据点击公司归属进入公司详细看板时：公司名
  let productDetailLineChartInstance = null;
  let productDetailStackedBarChartInstance = null;
  let companyDetailLineChartInstance = null;
  let companyDetailStackedBarChartInstance = null;
  let companyDetailProductRows = null;   // 公司产品汇总表原始行，用于排序后重绘
  let companyDetailProductMeta = null;   // { productColIdx, launchColIdx, ... }
  let companyDetailProductSortCol = -1;
  let companyDetailProductSortAsc = true;
  const CREATIVE_REGIONS = [{ key: '亚洲T1', label: '亚洲 T1 市场' }, { key: '欧美T1', label: '欧美 T1 市场' }, { key: 'T2', label: 'T2 市场' }, { key: 'T3', label: 'T3 市场' }];
  const YELLOW_BG = '#fff2cc';
  const CREATIVE_TAG_OPTIONS = ['数字门跑酷', '塔防', '肉鸽/幸存者 like/割草'];
  let advancedQueryLastResult = null;  // { headers, rows } 供下载表格
  let currentProductUnifiedId = '';    // 产品详情页当前产品的 Unified ID，供「拉取该产品 2.1/2.2 步」使用

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
    const hideUnifiedIdCol = isCompanyDimInitial || isProductDimension;
    headers.forEach((h, i) => {
      if (hideUnifiedIdCol && h === 'Unified ID') return;
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
      if ((isCompanyDimInitial || isProductDimension) && !isSummaryRow(row) && unifiedIdColIndexInitial >= 0 && row[unifiedIdColIndexInitial] != null && String(row[unifiedIdColIndexInitial]).trim() !== '') {
        tr.dataset.unifiedId = String(row[unifiedIdColIndexInitial]).trim();
      }
      row.forEach((cell, colIdx) => {
        if (hideUnifiedIdCol && colIdx === unifiedIdColIndexInitial) return;
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
    const isProductDim = currentDimension === 'product';
    const hideUnifiedIdInSort = isCompanyDim || isProductDim;
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
      row.forEach((cell, colIdx) => {
        if (hideUnifiedIdInSort && colIdx === unifiedIdColIndex) return;
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
    const url = DATA_API_BASE + '/formatted?year=' + encodeURIComponent(year) + '&week=' + encodeURIComponent(week);
    fetch(url, { credentials: 'include' })
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

  /** 产品维度子 tab 内容：大盘数据=筛选栏+表格，上线新游=搜索+空状态/表格，详细数据=请选择产品占位（与公司维度一致） */
  function showProductSubTabContent() {
    if (productNewGameWrap) hide(productNewGameWrap);
    if (productHomeDetailPlaceholder) hide(productHomeDetailPlaceholder);
    if (productToolbar) hide(productToolbar);
    hide(tableWrap);
    hide(loadingEl);
    hide(emptyEl);
    hide(errorEl);
    if (currentProductSubTab === 'new') {
      if (productNewGameWrap) show(productNewGameWrap);
      if (productTitleEl) {
        productTitleEl.textContent = (currentYear && currentWeek) ? currentYear + '年, ' + currentWeek + ', 上线新游' : '请从左侧选择周期（上线新游）';
      }
      loadNewProducts();
    } else if (currentProductSubTab === 'detail') {
      if (productHomeDetailPlaceholder) show(productHomeDetailPlaceholder);
    } else {
      if (productToolbar) show(productToolbar);
      show(tableWrap);
      if (currentData && currentDimension === 'product') renderTable(true);
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
    const url = DATA_API_BASE + '/product_strategy?year=' + encodeURIComponent(year) + '&week=' + encodeURIComponent(week) + '&type=' + key;
    fetch(url, { credentials: 'include' })
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

  /** 从开测时间字符串取日期部分 YYYY-MM-DD */
  function formatOpenTestDate(s) {
    if (s == null || s === '') return '—';
    var str = String(s).trim();
    if (str.length >= 10) return str.substring(0, 10);
    return str || '—';
  }

  /** 周标签 MMDD-MMDD（如 0119-0125）对应该周结束日 YYYY-MM-DD */
  function weekEndDate(year, weekTag) {
    if (!year || !weekTag || typeof weekTag !== 'string') return '';
    var parts = weekTag.split('-');
    if (parts.length < 2) return '';
    var endPart = parts[1].trim();
    if (endPart.length < 4) return '';
    var m = endPart.substring(0, 2);
    var d = endPart.substring(2, 4);
    return String(year) + '-' + m + '-' + d;
  }

  /** 产品名规范化后比较（trim、折叠空格、转小写），与总表 Unified Name 匹配用 */
  function normalizeProductNameForTotal(s) {
    if (s == null || s === '') return '';
    return String(s).trim().replace(/\s+/g, ' ').toLowerCase();
  }

  /** 仅用产品名在「包含或晚于开测日期」的周的总表 Unified Name 中匹配；有则返回 { inTotal: true, unifiedId }，否则 { inTotal: false, unifiedId: '' }。使用 normalizedSet/normalizedToId 做 O(1) 查找。 */
  function isProductInTotalByOpenTestDate(productName, openTestDateStr) {
    if (!productName || !newProductsMetricsByWeek || !newProductsMetricsByWeek.length) return { inTotal: false, unifiedId: '' };
    var nameNorm = normalizeProductNameForTotal(productName);
    if (!nameNorm) return { inTotal: false, unifiedId: '' };
    var compareDate = (openTestDateStr === '—' || !openTestDateStr) ? '' : String(openTestDateStr).trim();
    for (var i = 0; i < newProductsMetricsByWeek.length; i++) {
      var w = newProductsMetricsByWeek[i];
      var weekEndStr = weekEndDate(w.year, w.week);
      if (compareDate && weekEndStr && weekEndStr < compareDate) continue;
      if (w.normalizedSet && w.normalizedSet.has(nameNorm)) {
        var uid = (w.normalizedToId && w.normalizedToId[nameNorm]) ? String(w.normalizedToId[nameNorm]).trim() : '';
        return { inTotal: true, unifiedId: uid || '' };
      }
      var names = w.productNames || [];
      for (var j = 0; j < names.length; j++) {
        var n = names[j];
        if (normalizeProductNameForTotal(n) === nameNorm) {
          var uid = (w.nameToUnifiedId && w.nameToUnifiedId[n]) ? String(w.nameToUnifiedId[n]).trim() : '';
          return { inTotal: true, unifiedId: uid || '' };
        }
      }
    }
    return { inTotal: false, unifiedId: '' };
  }

  /** 为每周数据构建 normalizedSet（规范化产品名 Set）和 normalizedToId（规范化名 -> Unified ID），供 O(1) 匹配。 */
  function buildNormalizedLookupForWeek(w) {
    var names = w.productNames || [];
    var nameToId = w.nameToUnifiedId || {};
    var set = new Set();
    var toId = {};
    for (var i = 0; i < names.length; i++) {
      var n = names[i];
      var norm = normalizeProductNameForTotal(n);
      if (norm) {
        set.add(norm);
        var uid = nameToId[n];
        if (uid != null && String(uid).trim() !== '') toId[norm] = String(uid).trim();
      }
    }
    w.normalizedSet = set;
    w.normalizedToId = toId;
    return w;
  }

  function loadNewProducts() {
    if (!productNewGameWrap || !productNewGameTableHead || !productNewGameTableBody) return;
    if (productNewGameEmpty) hide(productNewGameEmpty);
    if (productNewGameTableWrap) hide(productNewGameTableWrap);
    var endProgress = maintenanceProgressStart('productNewGameProgress', 'productNewGameProgressPct');
    var dataBase = (document.location.pathname || '').indexOf('frontend') >= 0 ? '/frontend' : (typeof base !== 'undefined' ? base : '');
    var url = DATA_API_BASE + '/new_products?t=' + (Date.now ? Date.now() : 0);
    var mappingUrl = '/api/basetable?name=product_mapping';
    var metricsAllUrl = DATA_API_BASE + '/metrics_total_product_names_all';
    Promise.all([
      fetch(url).then(function (r) { return r.ok ? r.json() : { headers: [], rows: [] }; }).catch(function () { return { headers: [], rows: [] }; }),
      fetch(metricsAllUrl).then(function (r) { return r.ok ? r.json() : { weeks: [] }; }).catch(function () { return { weeks: [] }; }).then(function (res) {
        var list = res.weeks || [];
        list.forEach(function (w) { buildNormalizedLookupForWeek(w); });
        return list;
      }),
      fetch(mappingUrl).then(function (r) { return r.ok ? r.json() : { headers: [], rows: [] }; }).catch(function () { return { headers: [], rows: [] }; })
    ]).then(function (results) {
      endProgress();
      var data = results[0];
      var metricsList = results[1];
      var mappingData = results[2];
      newProductsData = { headers: data.headers || [], rows: data.rows || [] };
      var headers = newProductsData.headers || [];
      var allRows = newProductsData.rows || [];
      var weekColIdx = headers.indexOf('所属周');
      var weekKey = (currentYear && currentWeek) ? (String(currentYear) + '/' + String(currentWeek)) : '';
      if (weekColIdx >= 0 && weekKey) {
        newProductsWeekFilteredRows = allRows.filter(function (row) {
          var cell = row[weekColIdx];
          return cell != null && String(cell).trim() === weekKey;
        });
      } else {
        newProductsWeekFilteredRows = allRows.slice();
      }
      var q = (productNewGameSearch && productNewGameSearch.value) ? productNewGameSearch.value.trim().toLowerCase() : '';
      if (!q) {
        newProductsFilteredRows = newProductsWeekFilteredRows.slice();
      } else {
        newProductsFilteredRows = newProductsWeekFilteredRows.filter(function (row) {
          return row.some(function (cell) { return String(cell || '').toLowerCase().indexOf(q) >= 0; });
        });
      }
      if (productNewGameSearch) productNewGameSearch.value = q;
      newProductsMetricsByWeek = metricsList;
      newProductsInProductMapping = new Set();
      if (mappingData.rows && mappingData.headers) {
        var mapIdx = mappingData.headers.indexOf('产品归属');
        if (mapIdx >= 0) {
          mappingData.rows.forEach(function (row) {
            var v = row[mapIdx];
            if (v != null && String(v).trim() !== '') newProductsInProductMapping.add(String(v).trim());
          });
        } else if (mappingData.rows.length && mappingData.rows[0].length > 4) {
          mappingData.rows.forEach(function (row) {
            var v = row[4];
            if (v != null && String(v).trim() !== '') newProductsInProductMapping.add(String(v).trim());
          });
        }
      }
      renderNewProductsTable();
      if (newProductsFilteredRows && newProductsFilteredRows.length > 0) {
        if (productNewGameEmpty) hide(productNewGameEmpty);
        if (productNewGameTableWrap) show(productNewGameTableWrap);
      } else {
        if (productNewGameEmpty) show(productNewGameEmpty);
        if (productNewGameTableWrap) hide(productNewGameTableWrap);
      }
    });
  }

  function renderNewProductsTable() {
    if (!newProductsData || !productNewGameTableHead || !productNewGameTableBody) return;
    var headers = newProductsData.headers || [];
    var rows = newProductsFilteredRows || [];
    var idxName = headers.indexOf('产品名（实时更新中）');
    var idxBelong = headers.indexOf('产品归属');
    var idxTheme = headers.indexOf('题材');
    var idxStyle = headers.indexOf('画风');
    var idxPub = headers.indexOf('发行商');
    var idxComp = headers.indexOf('公司归属');
    var idxOpenTest = headers.indexOf('开测时间');
    var idxOffline = headers.indexOf('是否下架');
    var inMapping = newProductsInProductMapping || new Set();
    var showAddMappingBtn = currentUserRole === 'super_admin';
    productNewGameTableHead.innerHTML = '';
    var theadTr = document.createElement('tr');
    var displayHeaders = showAddMappingBtn ? NEW_PRODUCTS_DISPLAY : NEW_PRODUCTS_DISPLAY.slice(0, -1);
    displayHeaders.forEach(function (h) {
      var th = document.createElement('th');
      th.textContent = h;
      theadTr.appendChild(th);
    });
    productNewGameTableHead.appendChild(theadTr);
    productNewGameTableBody.innerHTML = '';
    rows.forEach(function (row, rowIdx) {
      var nameVal = idxName >= 0 && row[idxName] != null ? String(row[idxName]).trim() : '';
      var belongVal = idxBelong >= 0 && row[idxBelong] != null ? String(row[idxBelong]).trim() : '';
      var compVal = idxComp >= 0 && row[idxComp] != null ? String(row[idxComp]).trim() : '';
      if (!compVal) compVal = '未知';
      var openTestVal = idxOpenTest >= 0 && row[idxOpenTest] != null ? formatOpenTestDate(row[idxOpenTest]) : '—';
      var openTestDateStr = openTestVal === '—' ? '' : openTestVal;
      var offlineVal = idxOffline >= 0 && row[idxOffline] != null ? formatOpenTestDate(row[idxOffline]) : '—';
      var inTotalResult = isProductInTotalByOpenTestDate(nameVal, openTestDateStr);
      var inTotalVal = inTotalResult.inTotal ? '是' : '否';
      var belongOrName = (belongVal || nameVal || '').trim();
      var inMappingVal = belongOrName && inMapping.has(belongOrName) ? '是' : '否';
      var displayRow = [
        nameVal || belongVal || '—',
        belongVal || nameVal || '—',
        idxPub >= 0 && row[idxPub] != null ? String(row[idxPub]).trim() : '—',
        compVal,
        openTestVal,
        offlineVal,
        inTotalVal,
        inMappingVal
      ];
      var tr = document.createElement('tr');
      displayRow.forEach(function (cell, colIdx) {
        var td = document.createElement('td');
        td.textContent = cell != null && cell !== '' ? cell : '—';
        if ((colIdx === 6 || colIdx === 7) && cell === '是') td.classList.add('new-product-in-total-yes');
        tr.appendChild(td);
      });
      if (showAddMappingBtn) {
        var tdAction = document.createElement('td');
        var btn = document.createElement('button');
        btn.type = 'button';
        btn.className = 'btn-download btn-add-mapping';
        btn.textContent = '加入产品归属表';
        btn.dataset.rowIndex = String(rowIdx);
        tdAction.appendChild(btn);
        tr.appendChild(tdAction);
      }
      productNewGameTableBody.appendChild(tr);
    });
    productNewGameTableBody.querySelectorAll('.btn-add-mapping').forEach(function (btnEl) {
      btnEl.addEventListener('click', function () {
        var clickedBtn = this;
        var rowIdx = parseInt(clickedBtn.dataset.rowIndex, 10);
        if (!newProductsFilteredRows || rowIdx < 0 || rowIdx >= newProductsFilteredRows.length) return;
        var row = newProductsFilteredRows[rowIdx];
        var headers = newProductsData.headers || [];
        var idxName = headers.indexOf('产品名（实时更新中）');
        var idxBelong = headers.indexOf('产品归属');
        var idxTheme = headers.indexOf('题材');
        var idxStyle = headers.indexOf('画风');
        var idxPub = headers.indexOf('发行商');
        var idxComp = headers.indexOf('公司归属');
        var idxOpenTest = headers.indexOf('开测时间');
        var belongVal = idxBelong >= 0 && row[idxBelong] != null ? String(row[idxBelong]).trim() : '';
        var compVal = idxComp >= 0 && row[idxComp] != null ? String(row[idxComp]).trim() : '';
        if (!compVal) compVal = '未知';
        var prodName = idxName >= 0 && row[idxName] != null ? String(row[idxName]).trim() : belongVal;
        var openTestDateStr = idxOpenTest >= 0 && row[idxOpenTest] != null ? formatOpenTestDate(row[idxOpenTest]) : '';
        if (openTestDateStr === '—') openTestDateStr = '';
        var totalResult = isProductInTotalByOpenTestDate(prodName, openTestDateStr);
        var product = {
          产品名: prodName,
          产品归属: belongVal || prodName,
          'Unified ID': totalResult.unifiedId || '',
          题材: idxTheme >= 0 && row[idxTheme] != null ? String(row[idxTheme]).trim() : '',
          画风: idxStyle >= 0 && row[idxStyle] != null ? String(row[idxStyle]).trim() : '',
          发行商: idxPub >= 0 && row[idxPub] != null ? String(row[idxPub]).trim() : '',
          公司归属: compVal
        };
        if (!product.产品归属) {
          alert('该行无产品名与产品归属，无法加入。');
          return;
        }
        var origText = clickedBtn.textContent;
        clickedBtn.disabled = true;
        clickedBtn.textContent = '提交中...';
        fetch('/api/maintenance/add_to_product_mapping', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ products: [product] }),
          credentials: 'include'
        }).then(function (r) { return r.json(); }).then(function (res) {
          clickedBtn.disabled = false;
          clickedBtn.textContent = origText;
          alert(res.ok ? (res.message + (res.added != null ? ' 新增 ' + res.added + ' 条。' : '')) : res.message || '请求失败');
          if (res.ok) {
            newProductsInProductMapping = null;
            loadNewProducts();
          }
        }).catch(function (err) {
          clickedBtn.disabled = false;
          clickedBtn.textContent = origText;
          alert('请求失败: ' + (err.message || err));
        });
      });
    });
  }

  function loadBasetableContent(tab) {
    if (!basetableTableHead || !basetableTableBody) return;
    var dataBase = (document.location.pathname || '').indexOf('frontend') >= 0 ? '/frontend' : (typeof base !== 'undefined' ? base : '');
    if (basetablePeriodHint && basetablePeriodText) {
      if (tab === 'metrics_total' && currentYear && currentWeek) {
        basetablePeriodHint.style.display = '';
        basetablePeriodText.textContent = currentYear + '年 ' + currentWeek;
      } else {
        basetablePeriodHint.style.display = 'none';
      }
    }
    if (tab === 'metrics_total') {
      if (!currentYear || !currentWeek) {
        if (basetableMetricsToolbar) hide(basetableMetricsToolbar);
        if (basetableEmpty) { show(basetableEmpty); basetableEmpty.querySelector('.company-home-detail-prompt').textContent = '请从左侧选择周期（产品总表按周展示）'; }
        if (basetableTableWrap) hide(basetableTableWrap);
        return;
      }
      if (basetableMetricsToolbar) show(basetableMetricsToolbar);
      if (basetableOtherToolbar) hide(basetableOtherToolbar);
      var searchQ = (basetableMetricsSearch && basetableMetricsSearch.value) ? basetableMetricsSearch.value.trim() : '';
      var limit = searchQ ? 2000 : 1000;
      var apiUrl = DATA_API_BASE + '/metrics_total?year=' + encodeURIComponent(currentYear) + '&week=' + encodeURIComponent(currentWeek) + '&limit=' + limit + (searchQ ? '&q=' + encodeURIComponent(searchQ) : '');
      fetch(apiUrl, { credentials: 'include' }).then(function (r) { return r.ok ? r.json() : { headers: [], rows: [], total: 0 }; }).catch(function () { return { headers: [], rows: [], total: 0 }; })
        .then(function (data) {
          var headers = data.headers || [];
          var rows = data.rows || [];
          var total = data.total !== undefined ? data.total : rows.length;
          if (basetableEmpty) hide(basetableEmpty);
          if (basetableTableWrap) show(basetableTableWrap);
          basetableTableHead.innerHTML = '';
          var tr = document.createElement('tr');
          headers.forEach(function (h) { var th = document.createElement('th'); th.textContent = h; tr.appendChild(th); });
          basetableTableHead.appendChild(tr);
          basetableTableBody.innerHTML = '';
          rows.forEach(function (row) {
            var rowTr = document.createElement('tr');
            (row || []).forEach(function (cell) {
              var td = document.createElement('td');
              td.textContent = cell != null && cell !== '' ? String(cell) : '—';
              rowTr.appendChild(td);
            });
            basetableTableBody.appendChild(rowTr);
          });
          if (basetableMetricsHint) {
            if (searchQ) {
              basetableMetricsHint.textContent = '共 ' + total + ' 条匹配，当前展示前 ' + rows.length + ' 条（最多 2000 条）';
            } else {
              basetableMetricsHint.textContent = '共 ' + total + ' 条，当前展示前 ' + rows.length + ' 条，可搜索查看其他';
            }
          }
        });
      return;
    }
    if (basetableMetricsToolbar) hide(basetableMetricsToolbar);
    if (tab === 'new_products') {
      var newUrl = DATA_API_BASE + '/new_products';
      fetch(newUrl, { credentials: 'include' }).then(function (r) { return r.ok ? r.json() : { headers: [], rows: [] }; }).catch(function () { return { headers: [], rows: [] }; })
        .then(function (data) {
          var headers = data.headers || [];
          var rows = data.rows || [];
          basetableCachedData = { tab: tab, headers: headers, rows: rows };
          if (basetableOtherToolbar) show(basetableOtherToolbar);
          if (basetableOtherSearch) basetableOtherSearch.value = '';
          renderBasetableOtherTable();
        });
      return;
    }
    var apiUrl = '/api/basetable?name=' + encodeURIComponent(tab);
    fetch(apiUrl).then(function (r) { return r.ok ? r.json() : { headers: [], rows: [] }; }).catch(function () { return { headers: [], rows: [] }; })
      .then(function (data) {
        var headers = data.headers || [];
        var rows = data.rows || [];
        if (!headers.length && !rows.length) {
          basetableCachedData = null;
          if (basetableOtherToolbar) hide(basetableOtherToolbar);
          if (basetableEmpty) { show(basetableEmpty); basetableEmpty.querySelector('.company-home-detail-prompt').textContent = '暂无数据或文件不存在'; }
          if (basetableTableWrap) hide(basetableTableWrap);
        } else {
          basetableCachedData = { tab: tab, headers: headers, rows: rows };
          if (basetableOtherToolbar) show(basetableOtherToolbar);
          if (basetableOtherSearch) basetableOtherSearch.value = '';
          renderBasetableOtherTable();
        }
      });
  }

  function renderBasetableOtherTable() {
    if (!basetableCachedData || !basetableTableHead || !basetableTableBody) return;
    var headers = basetableCachedData.headers || [];
    var allRows = basetableCachedData.rows || [];
    var q = (basetableOtherSearch && basetableOtherSearch.value) ? basetableOtherSearch.value.trim().toLowerCase() : '';
    var rows = q ? allRows.filter(function (row) {
      return (row || []).some(function (cell) { return String(cell || '').toLowerCase().indexOf(q) >= 0; });
    }) : allRows;
    var hideColIdx = (basetableCachedData.tab === 'new_products') ? headers.indexOf('所属周') : -1;
    if (basetableEmpty) hide(basetableEmpty);
    if (basetableTableWrap) show(basetableTableWrap);
    basetableTableHead.innerHTML = '';
    var tr = document.createElement('tr');
    headers.forEach(function (h, i) {
      if (i === hideColIdx) return;
      var th = document.createElement('th');
      th.textContent = h;
      tr.appendChild(th);
    });
    basetableTableHead.appendChild(tr);
    basetableTableBody.innerHTML = '';
    rows.forEach(function (row) {
      var rowTr = document.createElement('tr');
      (row || []).forEach(function (cell, i) {
        if (i === hideColIdx) return;
        var td = document.createElement('td');
        td.textContent = cell != null && cell !== '' ? String(cell) : '—';
        rowTr.appendChild(td);
      });
      basetableTableBody.appendChild(rowTr);
    });
    if (basetableOtherHint) {
      if (q) {
        basetableOtherHint.textContent = '共 ' + allRows.length + ' 条，当前筛选出 ' + rows.length + ' 条';
      } else {
        basetableOtherHint.textContent = '共 ' + allRows.length + ' 条';
      }
    }
  }

  function loadWeek(year, week) {
    currentYear = year != null ? String(year) : null;
    currentWeek = week != null ? String(week) : null;
    setActiveWeekInSidebar(currentYear, currentWeek);
    if (currentDimension === 'company') loadCompanyWeek(year, week);
    else if (currentDimension === 'company-detail') loadCompanyDetail();
    else if (currentDimension === 'product') {
      if (currentProductSubTab === 'new') showProductSubTabContent();
      else loadProductWeek(year, week);
    }
    else if (currentDimension === 'product-detail') {
      // 看板固定，不随周切换自动刷新；用户点「刷新」时再更新
    }
    else if (currentDimension === 'creative') loadCreativeWeek(year, week);
    else if (currentDimension === 'combo') updateComboDisplay();
    else if (currentDimension === 'basetable' && currentBasetableTab === 'metrics_total') loadBasetableContent('metrics_total');
  }

  function loadCreativeProductsThen(year, week) {
    creativeTitleEl.textContent = year + '年, ' + week + ', 素材维度';
    if (creativePeriodHintEl) {
      creativePeriodHintEl.textContent = '当前周期与公司维度、产品维度一致：' + year + '年 ' + week;
      creativePeriodHintEl.style.display = '';
    }
    const url = DATA_API_BASE + '/creative_products?year=' + encodeURIComponent(year) + '&week=' + encodeURIComponent(week);
    fetch(url, { credentials: 'include' })
      .then(r => { if (!r.ok) throw new Error(r.statusText); return r.json(); })
      .then(index => {
        creativeProductsIndex = index;
        var fallbacks = [];
        if (!index.strategy_old || index.strategy_old.length === 0) {
          fallbacks.push(fetch(DATA_API_BASE + '/product_strategy?year=' + encodeURIComponent(year) + '&week=' + encodeURIComponent(week) + '&type=old', { credentials: 'include' }).then(r => r.ok ? r.json() : {}).then(data => {
            var headers = data.headers || [];
            var rows = data.rows || [];
            var col = headers.indexOf('产品归属');
            if (col >= 0 && rows.length) {
              index.strategy_old = rows.map(function (row) { return { product_name: String(row[col] || ''), display: String(row[col] || ''), folder: '', app_id: '', noCreative: true }; });
            }
          }));
        }
        if (!index.strategy_new || index.strategy_new.length === 0) {
          fallbacks.push(fetch(DATA_API_BASE + '/product_strategy?year=' + encodeURIComponent(year) + '&week=' + encodeURIComponent(week) + '&type=new', { credentials: 'include' }).then(r => r.ok ? r.json() : {}).then(data => {
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
    const url = DATA_API_BASE + '/product_strategy?year=' + encodeURIComponent(currentYear) + '&week=' + encodeURIComponent(currentWeek) + '&type=' + key;
    fetch(url, { credentials: 'include' }).then(r => r.ok ? r.json() : {}).then(data => {
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
    var y = year != null ? String(year) : '';
    var w = week != null ? String(week) : '';
    document.querySelectorAll('.sidebar-week-link').forEach(a => {
      a.classList.remove('active');
      if (String(a.dataset.year || '') === y && String(a.dataset.week || '') === w) a.classList.add('active');
    });
  }

  /** 从 URL hash 解析当前维度（唯一路由入口的输入） */
  function parseHash() {
    var h = (window.location.hash || '#company').replace('#', '').toLowerCase();
    return VALID_DIMS.indexOf(h) >= 0 ? h : 'company';
  }

  /** 仅更新 URL hash，不执行渲染；由 hashchange 触发 applyRoute，避免双向驱动 */
  function setRoute(dim) {
    if (window.location.hash !== '#' + dim) window.location.hash = '#' + dim;
  }

  /** 仅负责面板显示/隐藏与子导航状态，不加载数据 */
  function renderPanels(dim) {
    var navDim = dim === 'product-detail' ? 'product' : (dim === 'company-detail' ? 'company' : dim);
    if (dim === 'maintenance') navDim = 'maintenance';
    if (dim === 'approval') navDim = 'approval';
    document.querySelectorAll('.top-nav-link').forEach(l => { l.classList.toggle('active', l.dataset.dim === navDim); });
    if (dim === 'company') {
      show(panelCompany);
      hide(panelCompanyDetail);
      hide(panelProduct);
      hide(panelProductDetail);
      hide(panelCreative);
      hide(panelCombo);
      hide(panelBasetable);
      hide(panelMaintenance);
      hide(panelApproval);
      hide(panelAdvancedQuery);
      showCompanySubTabContent();
    } else if (dim === 'company-detail') {
      hide(panelCompany);
      show(panelCompanyDetail);
      hide(panelProduct);
      hide(panelProductDetail);
      hide(panelCreative);
      hide(panelCombo);
      hide(panelBasetable);
      hide(panelMaintenance);
      hide(panelApproval);
      hide(panelAdvancedQuery);
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
    } else if (dim === 'product') {
      hide(panelCompany);
      hide(panelCompanyDetail);
      show(panelProduct);
      hide(panelProductDetail);
      hide(panelCreative);
      hide(panelCombo);
      hide(panelBasetable);
      hide(panelMaintenance);
      hide(panelApproval);
      hide(panelAdvancedQuery);
      show(tableWrap);
      var productHomeNav = document.getElementById('productHomeSubNav');
      if (productHomeNav) {
        productHomeNav.querySelectorAll('.company-detail-sub-link').forEach(function (l) {
          l.classList.toggle('active', (l.dataset.tab || '') === currentProductSubTab);
        });
      }
      showProductSubTabContent();
    } else if (dim === 'product-detail') {
      hide(panelCompany);
      hide(panelCompanyDetail);
      hide(panelProduct);
      show(panelProductDetail);
      hide(panelCreative);
      hide(panelCombo);
      hide(panelBasetable);
      hide(panelMaintenance);
      hide(panelApproval);
      hide(panelAdvancedQuery);
      hide(tableWrap);
      hide(loadingEl);
      hide(emptyEl);
      hide(errorEl);
      var pdetailNav = document.getElementById('productDetailSubNav');
      if (pdetailNav) {
        pdetailNav.querySelectorAll('.product-detail-sub-link').forEach(function (l) { l.classList.remove('active'); });
        var productDetailTab = pdetailNav.querySelector('.product-detail-sub-link[data-tab="detail"]');
        if (productDetailTab) productDetailTab.classList.add('active');
      }
    } else if (dim === 'creative') {
      hide(panelCompany);
      hide(panelCompanyDetail);
      hide(panelProduct);
      hide(panelProductDetail);
      show(panelCreative);
      hide(panelCombo);
      hide(panelBasetable);
      hide(panelMaintenance);
      hide(panelApproval);
      hide(panelAdvancedQuery);
      show(tableWrap);
    } else if (dim === 'combo') {
      hide(panelCompany);
      hide(panelCompanyDetail);
      hide(panelProduct);
      hide(panelProductDetail);
      hide(panelCreative);
      show(panelCombo);
      hide(panelBasetable);
      hide(panelMaintenance);
      hide(panelApproval);
      hide(panelAdvancedQuery);
      hide(tableWrap);
      hide(loadingEl);
      hide(emptyEl);
      hide(errorEl);
    } else if (dim === 'basetable') {
      hide(panelCompany);
      hide(panelCompanyDetail);
      hide(panelProduct);
      hide(panelProductDetail);
      hide(panelCreative);
      hide(panelCombo);
      show(panelBasetable);
      hide(panelMaintenance);
      hide(panelApproval);
      hide(panelAdvancedQuery);
      hide(tableWrap);
      hide(loadingEl);
      hide(emptyEl);
      hide(errorEl);
      if (basetableEmpty) show(basetableEmpty);
      if (basetableTableWrap) hide(basetableTableWrap);
      if (basetableSubNav) {
        basetableSubNav.querySelectorAll('.company-detail-sub-link').forEach(function (l) {
          l.classList.toggle('active', l.getAttribute('data-tab') === currentBasetableTab);
        });
        if (!currentBasetableTab) {
          var first = basetableSubNav.querySelector('.company-detail-sub-link');
          if (first) {
            currentBasetableTab = first.getAttribute('data-tab');
            first.classList.add('active');
          }
        }
      }
    } else if (dim === 'maintenance') {
      hide(panelCompany);
      hide(panelCompanyDetail);
      hide(panelProduct);
      hide(panelProductDetail);
      hide(panelCreative);
      hide(panelCombo);
      hide(panelBasetable);
      show(panelMaintenance);
      hide(panelApproval);
      hide(panelAdvancedQuery);
      hide(tableWrap);
      hide(loadingEl);
      hide(emptyEl);
      hide(errorEl);
    } else if (dim === 'approval') {
      hide(panelCompany);
      hide(panelCompanyDetail);
      hide(panelProduct);
      hide(panelProductDetail);
      hide(panelCreative);
      hide(panelCombo);
      hide(panelBasetable);
      hide(panelMaintenance);
      show(panelApproval);
      hide(panelAdvancedQuery);
      hide(tableWrap);
      hide(loadingEl);
      hide(emptyEl);
      hide(errorEl);
      showApprovalView(currentApprovalTab);
      if (currentApprovalTab === 'pending' && typeof loadApprovalPendingUsers === 'function') loadApprovalPendingUsers();
      if (currentApprovalTab === 'existing' && typeof loadApprovalExistingUsers === 'function') loadApprovalExistingUsers();
    } else if (dim === 'advanced_query') {
      hide(panelCompany);
      hide(panelCompanyDetail);
      hide(panelProduct);
      hide(panelProductDetail);
      hide(panelCreative);
      hide(panelCombo);
      hide(panelBasetable);
      hide(panelMaintenance);
      hide(panelApproval);
      show(panelAdvancedQuery);
      hide(tableWrap);
      hide(loadingEl);
      hide(emptyEl);
      hide(errorEl);
      if (sidebarYearsEl) hide(sidebarYearsEl);
      if (sidebarAdvancedTables) show(sidebarAdvancedTables);
      if (advancedQueryEmpty) { show(advancedQueryEmpty); advancedQueryEmpty.textContent = '执行 SQL 或点击左侧表名查看数据'; }
      if (advancedQueryResultWrap) hide(advancedQueryResultWrap);
      if (typeof loadAdvancedQueryTables === 'function') loadAdvancedQueryTables();
    } else {
      hide(panelCompany);
      hide(panelCompanyDetail);
      show(panelProduct);
      hide(panelProductDetail);
      hide(panelCreative);
      hide(panelCombo);
      hide(panelBasetable);
      hide(panelMaintenance);
      hide(panelApproval);
      hide(panelAdvancedQuery);
      show(tableWrap);
    }
    var layoutEl = document.getElementById('layout');
    if (layoutEl) {
      layoutEl.classList.toggle('layout-maintenance', dim === 'maintenance');
      layoutEl.classList.toggle('layout-basetable-no-sidebar', dim === 'basetable' && currentBasetableTab !== 'metrics_total');
    }
    if (dim === 'advanced_query') {
      if (sidebarYearsEl) hide(sidebarYearsEl);
      if (sidebarApprovalTabs) hide(sidebarApprovalTabs);
      if (sidebarAdvancedTables) show(sidebarAdvancedTables);
    } else if (dim === 'approval') {
      if (sidebarYearsEl) hide(sidebarYearsEl);
      if (sidebarApprovalTabs) show(sidebarApprovalTabs);
      if (sidebarAdvancedTables) hide(sidebarAdvancedTables);
    } else {
      if (sidebarYearsEl) show(sidebarYearsEl);
      if (sidebarApprovalTabs) hide(sidebarApprovalTabs);
      if (sidebarAdvancedTables) hide(sidebarAdvancedTables);
    }
    if (dim === 'basetable' && currentBasetableTab) {
      loadBasetableContent(currentBasetableTab);
      if (currentBasetableTab === 'metrics_total' && currentYear && currentWeek && typeof setActiveWeekInSidebar === 'function') setActiveWeekInSidebar(currentYear, currentWeek);
    }
  }

  /** 仅负责按当前维度加载数据 */
  function loadDataForDimension(dim) {
    if (dim === 'company') {
      if (currentYear && currentWeek) loadCompanyWeek(currentYear, currentWeek);
      else setState('empty');
    } else if (dim === 'company-detail') {
      if (currentYear && currentWeek && selectedCompanyForDetail) loadCompanyDetail();
      else renderCompanyDetailPlaceholder();
    } else if (dim === 'product') {
      if (currentProductSubTab === 'new') {
        showProductSubTabContent();
      } else if (currentYear && currentWeek) {
        loadProductWeek(currentYear, currentWeek);
      } else {
        setState('empty');
      }
    } else if (dim === 'product-detail') {
      if (currentYear && currentWeek && selectedProductForDetail) loadProductDetail();
      else renderProductDetailPlaceholder();
    } else if (dim === 'creative') {
      if (currentYear && currentWeek) loadCreativeWeek(currentYear, currentWeek);
      else {
        creativeTitleEl.textContent = '请从左侧选择周期';
        creativeProductSelect.innerHTML = '<option value="">-- 请先选择周期 --</option>';
        creativeProductDataRow.style.display = 'none';
        setState('empty');
      }
    } else if (dim === 'combo') {
      updateComboDisplay();
    } else if (dim === 'basetable') {
      if (currentBasetableTab) loadBasetableContent(currentBasetableTab);
      else if (basetableEmpty) { show(basetableEmpty); hide(basetableTableWrap); }
    } else if (dim === 'maintenance') {
      /* 数据维护页无需预加载数据 */
    } else if (dim === 'approval') {
      /* 用户审批列表在 renderPanels 中调用 loadApprovalPendingUsers */
    } else {
      setState('empty');
    }
  }

  /** 唯一的路由应用入口：同步状态、渲染面板、加载数据；仅由 hashchange 或初始化调用，不直接写 hash */
  function applyRoute(dim) {
    if (VALID_DIMS.indexOf(dim) < 0) dim = 'company';
    if ((dim === 'maintenance' || dim === 'approval' || dim === 'advanced_query') && currentUserRole !== 'super_admin') {
      dim = 'company';
      if (window.location.hash !== '#company') window.location.hash = '#company';
    }
    currentDimension = dim;
    if (dim !== 'product-detail') destroyProductDetailCharts();
    if (dim !== 'company-detail') destroyCompanyTrendCharts();
    renderPanels(dim);
    loadDataForDimension(dim);
  }

  /** 统一跳转：仅改 hash，由 hashchange 触发 applyRoute */
  function goToDimension(dim) {
    setRoute(dim);
  }

  /** 打开公司详情：先写入选中的公司，再改路由 */
  function openCompanyDetail(company) {
    selectedCompanyForDetail = company;
    setRoute('company-detail');
  }

  /** 打开产品详情：先写入产品和来源，再改路由 */
  function openProductDetail(productInfo, origin) {
    selectedProductForDetail = productInfo;
    productDetailOrigin = origin || 'product';
    setRoute('product-detail');
  }

  /** 打开素材维度，可选预选产品 */
  function openCreative(pendingProduct, productKey) {
    pendingCreativeProduct = pendingProduct || null;
    if (creativeProductTable && productKey) creativeProductTable.value = 'strategy_' + (productKey === 'new' ? 'new' : 'old');
    setRoute('creative');
  }

  /** @deprecated 仅保留供 buildSidebar 等内部在“不触发 hash”的场景下切维度，其余请用 goToDimension/openCompanyDetail/openProductDetail/openCreative + setRoute */
  function switchDimension(dim) {
    applyRoute(dim);
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
    var placeholderEl = document.getElementById('productDetailPlaceholder');
    var contentEl = document.getElementById('productDetailContent');
    if (titleEl) titleEl.textContent = '产品详细信息';
    if (dateEl) dateEl.textContent = '—';
    if (placeholderEl) show(placeholderEl);
    if (contentEl) hide(contentEl);
    ['productDetailCompany', 'productDetailNewOld', 'productDetailLaunch', 'productDetailUnifiedId', 'productDetailTheme', 'productDetailStyle', 'productDetailInstall', 'productDetailRankInstall', 'productDetailRevenue', 'productDetailRankRevenue'].forEach(function (id) {
      var el = document.getElementById(id);
      if (el) el.textContent = '—';
    });
    var copyBtn = document.getElementById('productDetailUnifiedIdCopy');
    if (copyBtn) copyBtn.style.display = 'none';
    currentProductUnifiedId = '';
    var singlePullWrap = document.getElementById('productDetailSinglePullWrap');
    if (singlePullWrap) singlePullWrap.style.display = 'none';
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
    companyDetailProductRows = null;
    companyDetailProductMeta = null;
    companyDetailProductSortCol = -1;
    companyDetailProductSortAsc = true;
  }

  /** 从变动列原始值解析数值用于排序：如 "31.81%▲" -> 31.81，"-16.51%▼" -> -16.51，"inf%▲" -> 大数 */
  function parsePctForSort(val) {
    if (val == null || val === '') return null;
    var n = Number(val);
    if (!Number.isNaN(n)) return n;
    var s = String(val).trim();
    var infMatch = /inf|∞/i.test(s);
    if (infMatch) return s.indexOf('-') >= 0 ? -Infinity : Infinity;
    var m = s.match(/([+-]?\d+\.?\d*)\s*%?/);
    return m ? parseFloat(m[1]) : null;
  }

  /** 按当前排序状态重绘公司产品汇总表 */
  function renderCompanyDetailProductTable() {
    var tbody = document.getElementById('companyDetailProductBody');
    if (!tbody || !companyDetailProductRows || !companyDetailProductMeta) return;
    var meta = companyDetailProductMeta;
    var productColIdx = meta.productColIdx;
    var launchColIdx = meta.launchColIdx;
    var latestInstallColIdx = meta.latestInstallColIdx;
    var installChangeColIdx = meta.installChangeColIdx;
    var latestRevenueColIdx = meta.latestRevenueColIdx;
    var revenueChangeColIdx = meta.revenueChangeColIdx;
    var unifiedIdColIdx = meta.unifiedIdColIdx;
    var rows = companyDetailProductRows.slice();
    var col = companyDetailProductSortCol;
    var asc = companyDetailProductSortAsc;
    if (col >= 0 && col <= 5) {
      rows.sort(function (a, b) {
        var av, bv;
        if (col === 0) {
          av = productColIdx >= 0 ? (a[productColIdx] != null ? String(a[productColIdx]) : '') : '';
          bv = productColIdx >= 0 ? (b[productColIdx] != null ? String(b[productColIdx]) : '') : '';
          return asc ? (av || '').localeCompare(bv || '') : (bv || '').localeCompare(av || '');
        }
        if (col === 1) {
          av = launchColIdx >= 0 ? (a[launchColIdx] != null ? String(a[launchColIdx]).replace(/\s+\d{2}:\d{2}:\d{2}$/, '') : '') : '';
          bv = launchColIdx >= 0 ? (b[launchColIdx] != null ? String(b[launchColIdx]).replace(/\s+\d{2}:\d{2}:\d{2}$/, '') : '') : '';
          return asc ? (av || '').localeCompare(bv || '') : (bv || '').localeCompare(av || '');
        }
        if (col === 2) {
          av = latestInstallColIdx >= 0 ? (Number(a[latestInstallColIdx]) || 0) : 0;
          bv = latestInstallColIdx >= 0 ? (Number(b[latestInstallColIdx]) || 0) : 0;
          return asc ? av - bv : bv - av;
        }
        if (col === 3) {
          av = parsePctForSort(a[installChangeColIdx]);
          bv = parsePctForSort(b[installChangeColIdx]);
          if (av == null && bv == null) return 0;
          if (av == null) return asc ? 1 : -1;
          if (bv == null) return asc ? -1 : 1;
          return asc ? av - bv : bv - av;
        }
        if (col === 4) {
          av = latestRevenueColIdx >= 0 ? (Number(a[latestRevenueColIdx]) || 0) : 0;
          bv = latestRevenueColIdx >= 0 ? (Number(b[latestRevenueColIdx]) || 0) : 0;
          return asc ? av - bv : bv - av;
        }
        if (col === 5) {
          av = parsePctForSort(a[revenueChangeColIdx]);
          bv = parsePctForSort(b[revenueChangeColIdx]);
          if (av == null && bv == null) return 0;
          if (av == null) return asc ? 1 : -1;
          if (bv == null) return asc ? -1 : 1;
          return asc ? av - bv : bv - av;
        }
        return 0;
      });
    }
    document.querySelectorAll('.company-product-table thead th').forEach(function (th) {
      var c = th.dataset.col != null ? parseInt(th.dataset.col, 10) : -1;
      th.dataset.sort = (c === col && col >= 0) ? (asc ? 'asc' : 'desc') : '';
    });
    tbody.innerHTML = '';
    rows.forEach(function (r) {
      var tr = document.createElement('tr');
      var productName = productColIdx >= 0 ? (r[productColIdx] != null ? String(r[productColIdx]) : '—') : '—';
      var launch = launchColIdx >= 0 ? (r[launchColIdx] != null ? String(r[launchColIdx]).replace(/\s+\d{2}:\d{2}:\d{2}$/, '') : '—') : '—';
      var latestInstall = latestInstallColIdx >= 0 && r[latestInstallColIdx] != null && r[latestInstallColIdx] !== ''
        ? (Number(r[latestInstallColIdx]) || 0).toLocaleString('en-US') : '—';
      var latestRevenue = latestRevenueColIdx >= 0 && r[latestRevenueColIdx] != null && r[latestRevenueColIdx] !== ''
        ? '$' + (Math.round(Number(r[latestRevenueColIdx])) || 0).toLocaleString('en-US') : '—';
      var installChangeRaw = installChangeColIdx >= 0 ? (r[installChangeColIdx] != null ? formatCell('周安装变动', r[installChangeColIdx]) : '—') : '—';
      var revenueChangeRaw = revenueChangeColIdx >= 0 ? (r[revenueChangeColIdx] != null ? formatCell('周流水变动', r[revenueChangeColIdx]) : '—') : '—';
      var installChangeClass = (String(installChangeRaw).indexOf('▼') >= 0) ? 'cell-down' : (String(installChangeRaw).indexOf('▲') >= 0 ? 'cell-up' : '');
      var revenueChangeClass = (String(revenueChangeRaw).indexOf('▼') >= 0) ? 'cell-down' : (String(revenueChangeRaw).indexOf('▲') >= 0 ? 'cell-up' : '');
      var productLinkHtml = productName !== '—' ? '<a href="#" class="product-link company-detail-product-link">' + escapeHtml(productName) + '</a>' : escapeHtml(productName);
      var unifiedId = unifiedIdColIdx >= 0 && r[unifiedIdColIdx] != null ? String(r[unifiedIdColIdx]).trim() : null;
      tr.innerHTML = '<td>' + productLinkHtml + '</td><td>' + escapeHtml(launch) + '</td><td>' + escapeHtml(latestInstall) + '</td><td class="' + installChangeClass + '">' + escapeHtml(installChangeRaw) + '</td><td>' + escapeHtml(latestRevenue) + '</td><td class="' + revenueChangeClass + '">' + escapeHtml(revenueChangeRaw) + '</td>';
      if (productName !== '—') {
        var linkEl = tr.querySelector('.company-detail-product-link');
        if (linkEl) {
          linkEl.dataset.productName = productName;
          if (unifiedId) linkEl.dataset.unifiedId = unifiedId;
        }
      }
      tbody.appendChild(tr);
    });
  }

  function evictDetailCache(type) {
    var obj = detailCache[type];
    var keys = Object.keys(obj);
    if (keys.length > DETAIL_CACHE_MAX) delete obj[keys[0]];
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
    var companyCacheKey = currentYear + '|' + currentWeek + '|' + String(selectedCompanyForDetail || '').trim();
    var cached = detailCache.company[companyCacheKey];
    if (cached && cached.formatted && cached.panels) {
      var headers = cached.formatted.headers || [];
      var rows = cached.formatted.rows || [];
      var companyColIdx = headers.indexOf('公司归属');
      var productColIdx = headers.indexOf('产品归属');
      var unifiedIdColIdx = headers.indexOf('Unified ID');
      var launchColIdx = headers.indexOf('第三方记录最早上线时间');
      var latestInstallColIdx = headers.indexOf('当周周安装');
      var installChangeColIdx = headers.indexOf('周安装变动');
      var latestRevenueColIdx = headers.indexOf('当周周流水');
      var revenueChangeColIdx = headers.indexOf('周流水变动');
      var companyRows = companyColIdx >= 0 ? rows.filter(function (r) {
        var c = r[companyColIdx];
        return c != null && String(c).trim() === String(selectedCompanyForDetail).trim();
      }) : [];
      companyDetailProductRows = companyRows;
      companyDetailProductMeta = { productColIdx: productColIdx, launchColIdx: launchColIdx, latestInstallColIdx: latestInstallColIdx, installChangeColIdx: installChangeColIdx, latestRevenueColIdx: latestRevenueColIdx, revenueChangeColIdx: revenueChangeColIdx, unifiedIdColIdx: unifiedIdColIdx };
      renderCompanyDetailProductTable();
      var installEl = document.getElementById('companyDetailInstall');
      var revenueEl = document.getElementById('companyDetailRevenue');
      var rankInstallEl = document.getElementById('companyDetailRankInstall');
      var rankRevenueEl = document.getElementById('companyDetailRankRevenue');
      if (installEl) installEl.textContent = (cached.panels.sumInstall != null && !Number.isNaN(cached.panels.sumInstall)) ? Number(cached.panels.sumInstall).toLocaleString('en-US') : '—';
      if (revenueEl) revenueEl.textContent = (cached.panels.sumRevenue != null && !Number.isNaN(cached.panels.sumRevenue)) ? '$' + Number(cached.panels.sumRevenue).toLocaleString('en-US', { minimumFractionDigits: 0, maximumFractionDigits: 0 }) : '—';
      if (rankInstallEl) rankInstallEl.textContent = (cached.panels.rankInstall != null && cached.panels.rankInstall > 0) ? '第' + cached.panels.rankInstall + '名' : '—';
      if (rankRevenueEl) rankRevenueEl.textContent = (cached.panels.rankRevenue != null && cached.panels.rankRevenue > 0) ? '第' + cached.panels.rankRevenue + '名' : '—';
      loadCompanyTrendCharts();
      return;
    }
    var url = DATA_API_BASE + '/formatted?year=' + encodeURIComponent(currentYear) + '&week=' + encodeURIComponent(currentWeek);
    fetch(url, { credentials: 'include' })
      .then(function (r) { if (!r.ok) throw new Error(r.statusText || '加载失败'); return r.json(); })
      .then(function (data) {
        var headers = data.headers || [];
        var rows = data.rows || [];
        var companyColIdx = headers.indexOf('公司归属');
        var productColIdx = headers.indexOf('产品归属');
        var unifiedIdColIdx = headers.indexOf('Unified ID');
        var launchColIdx = headers.indexOf('第三方记录最早上线时间');
        var latestInstallColIdx = headers.indexOf('当周周安装');
        var installChangeColIdx = headers.indexOf('周安装变动');
        var latestRevenueColIdx = headers.indexOf('当周周流水');
        var revenueChangeColIdx = headers.indexOf('周流水变动');
        var companyRows = companyColIdx >= 0 ? rows.filter(function (r) {
          var c = r[companyColIdx];
          return c != null && String(c).trim() === String(selectedCompanyForDetail).trim();
        }) : [];
        companyDetailProductRows = companyRows;
        companyDetailProductMeta = {
          productColIdx: productColIdx,
          launchColIdx: launchColIdx,
          latestInstallColIdx: latestInstallColIdx,
          installChangeColIdx: installChangeColIdx,
          latestRevenueColIdx: latestRevenueColIdx,
          revenueChangeColIdx: revenueChangeColIdx,
          unifiedIdColIdx: unifiedIdColIdx
        };
        renderCompanyDetailProductTable();
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
        var companyPanelsUrl = DATA_API_BASE + '/company_detail_panels?year=' + encodeURIComponent(currentYear) + '&week=' + encodeURIComponent(currentWeek) + '&company=' + encodeURIComponent(String(selectedCompanyForDetail || '').trim());
        fetch(companyPanelsUrl, { credentials: 'include' }).then(function (r) { return r.ok ? r.json() : null; }).catch(function () { return null; })
          .then(function (panelsData) {
            if (panelsData && (panelsData.sumInstall != null || panelsData.sumRevenue != null || panelsData.rankInstall != null || panelsData.rankRevenue != null)) {
              setCompanyCumulative(panelsData.sumInstall != null ? panelsData.sumInstall : null, panelsData.sumRevenue != null ? panelsData.sumRevenue : null);
              setCompanyRank(panelsData.rankInstall != null ? panelsData.rankInstall : null, panelsData.rankRevenue != null ? panelsData.rankRevenue : null);
              detailCache.company[companyCacheKey] = { formatted: data, panels: panelsData };
              evictDetailCache('company');
              loadCompanyTrendCharts();
              return;
            }
            return tryNextMetrics(0);
          });
        function tryNextMetrics(i) {
          if (i >= pairs.length) {
            setCompanyCumulative(null, null);
            setCompanyRank(null, null);
            loadCompanyTrendCharts();
            return Promise.resolve();
          }
          var p = pairs[i];
          return fetch(DATA_API_BASE + '/metrics_total?year=' + encodeURIComponent(p.year) + '&week=' + encodeURIComponent(p.weekTag) + '&limit=999999')
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
              detailCache.company[companyCacheKey] = { formatted: data, panels: { sumInstall: sumInstall, sumRevenue: sumRevenue, rankInstall: rankInstall, rankRevenue: rankRevenue } };
              evictDetailCache('company');
              loadCompanyTrendCharts();
              return Promise.resolve();
            });
        }
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
    fetch(DATA_API_BASE + '/weeks_index', { credentials: 'include' })
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
    var placeholderEl = document.getElementById('productDetailPlaceholder');
    var contentEl = document.getElementById('productDetailContent');
    if (placeholderEl) hide(placeholderEl);
    if (contentEl) show(contentEl);
    var titleEl = document.getElementById('productDetailTitle');
    var dateEl = document.getElementById('productDetailDateRange');
    if (titleEl) titleEl.textContent = selectedProductForDetail.name || '产品详细信息';
    if (dateEl) dateEl.textContent = weekTagToSlashDateRange(currentYear, currentWeek);
    var productNameForPanels = (selectedProductForDetail && selectedProductForDetail.name) ? String(selectedProductForDetail.name).trim() : '';
    var unifiedIdForPanels = (selectedProductForDetail && selectedProductForDetail.unifiedId) ? String(selectedProductForDetail.unifiedId).trim() : '';
    var productCacheKey = currentYear + '|' + currentWeek + '|' + productNameForPanels + '|' + (unifiedIdForPanels || '');
    var productCached = detailCache.product[productCacheKey];
    if (productCached && productCached.panels && (productCached.panels.install != null || productCached.panels.revenue != null || productCached.panels.company || productCached.panels.unifiedId)) {
      var emptyData = { headers: [], rows: [], dataOld: { headers: [], rows: [] }, dataNew: { headers: [], rows: [] } };
      runProductDetailLogic(emptyData, { headers: [], rows: [] }, { precomputedPanels: true, panels: productCached.panels });
      loadProductTrendCharts();
      return;
    }
    if (!productThemeStyleMapping && typeof window.__PRODUCT_THEME_STYLE_MAPPING__ !== 'undefined' && window.__PRODUCT_THEME_STYLE_MAPPING__ != null) productThemeStyleMapping = window.__PRODUCT_THEME_STYLE_MAPPING__;
    var panelsUrl = DATA_API_BASE + '/product_detail_panels?year=' + encodeURIComponent(currentYear) + '&week=' + encodeURIComponent(currentWeek) + (unifiedIdForPanels ? '&unified_id=' + encodeURIComponent(unifiedIdForPanels) : '') + (productNameForPanels ? '&product_name=' + encodeURIComponent(productNameForPanels) : '');
    // 优先一次请求取回面板所需数据（后端从 metrics_total 为主聚合），成功则不再请求 strategy/formatted
    fetch(panelsUrl, { credentials: 'include' }).then(function (r) { return r.ok ? r.json() : null; }).catch(function () { return null; })
      .then(function (panelsData) {
        if (panelsData && (panelsData.install != null || panelsData.revenue != null || panelsData.company || panelsData.unifiedId)) {
          var emptyData = { headers: [], rows: [], dataOld: { headers: [], rows: [] }, dataNew: { headers: [], rows: [] } };
          detailCache.product[productCacheKey] = { panels: panelsData };
          evictDetailCache('product');
          return runProductDetailLogic(emptyData, { headers: [], rows: [] }, { precomputedPanels: true, panels: panelsData });
        }
        var urlOld = base + '/data/' + currentYear + '/' + currentWeek + '/product_strategy_old.json';
        var urlNew = base + '/data/' + currentYear + '/' + currentWeek + '/product_strategy_new.json';
        var urlFormatted = DATA_API_BASE + '/formatted?year=' + encodeURIComponent(currentYear) + '&week=' + encodeURIComponent(currentWeek);
        return Promise.all([
          fetch(urlOld, { credentials: 'include' }).then(function (r) { return r.ok ? r.json() : { headers: [], rows: [] }; }),
          fetch(urlNew, { credentials: 'include' }).then(function (r) { return r.ok ? r.json() : { headers: [], rows: [] }; }),
          fetch(urlFormatted, { credentials: 'include' }).then(function (r) { if (!r.ok) return { headers: [], rows: [] }; return r.json(); })
        ]).then(function (results) {
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
            return fetch(DATA_API_BASE + '/metrics_total?year=' + encodeURIComponent(p.year) + '&week=' + encodeURIComponent(p.weekTag) + '&limit=999999')
              .then(function (r) { return r.ok ? r.json() : null; })
              .catch(function () { return null; })
              .then(function (metrics) {
                if (!metrics || !metrics.rows || !metrics.rows.length || !metrics.headers) return tryNextMetrics(i + 1);
                return runProductDetailLogic(data, formattedData, metrics);
              });
          }
          return tryNextMetrics(0);
        });
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
        var resolvedUnifiedId = selectedProductForDetail.unifiedId ? String(selectedProductForDetail.unifiedId).trim() : '';
        var key = selectedProductForDetail.key || 'old';
        if (metricsData && metricsData.precomputedPanels && metricsData.panels) {
          var panels = metricsData.panels;
          if (panels.unifiedId) { resolvedUnifiedId = panels.unifiedId; selectedProductForDetail.unifiedId = panels.unifiedId; }
          if (panels.newOld) key = panels.newOld;
          var companyEl = document.getElementById('productDetailCompany');
          var newOldEl = document.getElementById('productDetailNewOld');
          var launchEl = document.getElementById('productDetailLaunch');
          var installEl = document.getElementById('productDetailInstall');
          var rankInstallEl = document.getElementById('productDetailRankInstall');
          var revenueEl = document.getElementById('productDetailRevenue');
          var rankRevenueEl = document.getElementById('productDetailRankRevenue');
          var unifiedIdEl = document.getElementById('productDetailUnifiedId');
          var unifiedIdCopyBtn = document.getElementById('productDetailUnifiedIdCopy');
          var unifiedIdVal = resolvedUnifiedId || '';
          if (companyEl) companyEl.textContent = panels.company || '—';
          if (newOldEl) newOldEl.textContent = key === 'old' ? '旧产品' : '新产品';
          if (launchEl) launchEl.textContent = (panels.launch != null && panels.launch !== '') ? String(panels.launch).replace(/\s+\d{2}:\d{2}:\d{2}$/, '') : '—';
          if (installEl) {
            if (panels.install != null && panels.install !== '') {
              var n = typeof panels.install === 'number' ? panels.install : (parseFloat(String(panels.install).replace(/,/g, '')) || parseInt(String(panels.install).replace(/,/g, ''), 10));
              installEl.textContent = !Number.isNaN(n) ? n.toLocaleString('en-US') : '—';
            } else installEl.textContent = '—';
          }
          if (rankInstallEl) rankInstallEl.textContent = panels.rankInstall != null ? String(panels.rankInstall) : '—';
          if (revenueEl) {
            if (panels.revenue != null && panels.revenue !== '') {
              var rStr = String(panels.revenue).replace(/[$,]/g, '');
              var rn = typeof panels.revenue === 'number' ? panels.revenue : (parseFloat(rStr) || parseInt(rStr, 10));
              revenueEl.textContent = !Number.isNaN(rn) ? '$' + rn.toLocaleString('en-US', { minimumFractionDigits: 0, maximumFractionDigits: 0 }) : '—';
            } else revenueEl.textContent = '—';
          }
          if (rankRevenueEl) rankRevenueEl.textContent = panels.rankRevenue != null ? String(panels.rankRevenue) : '—';
          if (unifiedIdEl) unifiedIdEl.textContent = unifiedIdVal || '—';
          if (unifiedIdCopyBtn) { unifiedIdCopyBtn.style.display = unifiedIdVal ? '' : 'none'; unifiedIdCopyBtn.dataset.unifiedId = unifiedIdVal; }
          currentProductUnifiedId = unifiedIdVal || '';
          var singlePullWrap = document.getElementById('productDetailSinglePullWrap');
          if (singlePullWrap) singlePullWrap.style.display = currentProductUnifiedId ? '' : 'none';
          var row = null;
          for (var i = 0; i < rows.length; i++) {
            var r = rows[i];
            if (unifiedIdColIdx >= 0 && r[unifiedIdColIdx] != null && String(r[unifiedIdColIdx]).trim() === resolvedUnifiedId) { row = r; break; }
            var cell = productColIdx >= 0 ? r[productColIdx] : '';
            if (productNamesMatch(selectedProductForDetail.name, cell, cell)) { row = r; break; }
          }
          var themeEl = document.getElementById('productDetailTheme');
          var styleEl = document.getElementById('productDetailStyle');
          var themeVal = (panels.theme != null && panels.theme !== '') ? String(panels.theme) : null;
          var styleVal = (panels.style != null && panels.style !== '') ? String(panels.style) : null;
          if (!themeVal && productThemeStyleMapping && resolvedUnifiedId && productThemeStyleMapping.byUnifiedId && productThemeStyleMapping.byUnifiedId[resolvedUnifiedId]) {
            var e = productThemeStyleMapping.byUnifiedId[resolvedUnifiedId];
            themeVal = (e && (e.theme || e['题材'])) ? String(e.theme || e['题材']) : null;
            if (!styleVal && e) styleVal = (e.style || e['画风']) ? String(e.style || e['画风']) : null;
          }
          if (!themeVal && productThemeStyleMapping && productThemeStyleMapping.byProductName) {
            var pname = selectedProductForDetail.name ? String(selectedProductForDetail.name).trim() : '';
            if (pname && productThemeStyleMapping.byProductName[pname]) {
              var e2 = productThemeStyleMapping.byProductName[pname];
              themeVal = (e2 && (e2.theme || e2['题材'])) ? String(e2.theme || e2['题材']) : themeVal;
              if (!styleVal && e2) styleVal = (e2.style || e2['画风']) ? String(e2.style || e2['画风']) : styleVal;
            }
          }
          if (themeEl) themeEl.textContent = themeVal || '—';
          if (styleEl) styleEl.textContent = styleVal || '—';
          return;
        }
        // 从哪进入都统一：先按产品名在 old 再在 new strategy 里解析 Unified ID 与 key（先精确再模糊）
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
        var unifiedIdEl = document.getElementById('productDetailUnifiedId');
        var unifiedIdCopyBtn = document.getElementById('productDetailUnifiedIdCopy');
        var unifiedIdVal = resolvedUnifiedId || getVal('Unified ID');
        if (unifiedIdVal != null && String(unifiedIdVal).trim() !== '') unifiedIdVal = String(unifiedIdVal).trim();
        else unifiedIdVal = '';
        if (unifiedIdEl) unifiedIdEl.textContent = unifiedIdVal || '—';
        if (unifiedIdCopyBtn) {
          unifiedIdCopyBtn.style.display = unifiedIdVal ? '' : 'none';
          unifiedIdCopyBtn.dataset.unifiedId = unifiedIdVal;
        }
        currentProductUnifiedId = unifiedIdVal || '';
        var singlePullWrap = document.getElementById('productDetailSinglePullWrap');
        if (singlePullWrap) singlePullWrap.style.display = currentProductUnifiedId ? '' : 'none';
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
    fetch(DATA_API_BASE + '/weeks_index', { credentials: 'include' })
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
          var urlTotal = DATA_API_BASE + '/formatted?year=' + encodeURIComponent(p.year) + '&week=' + encodeURIComponent(p.weekTag);
          var urlStrategyOld = DATA_API_BASE + '/product_strategy?year=' + encodeURIComponent(p.year) + '&week=' + encodeURIComponent(p.weekTag) + '&type=old';
          var urlStrategyNew = DATA_API_BASE + '/product_strategy?year=' + encodeURIComponent(p.year) + '&week=' + encodeURIComponent(p.weekTag) + '&type=new';
          Promise.all([
            fetch(urlTotal, { credentials: 'include' }).then(function (r) { return r.ok ? r.json() : { headers: [], rows: [] }; }),
            fetch(urlStrategyOld, { credentials: 'include' }).then(function (r) { return r.ok ? r.json() : { headers: [], rows: [] }; }),
            fetch(urlStrategyNew, { credentials: 'include' }).then(function (r) { return r.ok ? r.json() : { headers: [], rows: [] }; })
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
    }
    var dim = parseHash();
    if (VALID_DIMS.indexOf(dim) < 0) dim = 'company';
    applyRoute(dim);
    if (years.length === 0 || !allPairs.length) setState('empty');
  }

  function loadWeeksIndex() {
    setState('loading');
    // 优先使用 start_server 注入的周索引（访问 /frontend/ 时服务器会注入），不依赖 fetch
    var dataBase = (document.location.pathname || '').indexOf('frontend') >= 0 ? '/frontend' : base;
    if (typeof window.__WEEKS_INDEX__ !== 'undefined' && window.__WEEKS_INDEX__ != null) {
      try {
        buildSidebar(window.__WEEKS_INDEX__);
        if (typeof window.__PRODUCT_THEME_STYLE_MAPPING__ !== 'undefined' && window.__PRODUCT_THEME_STYLE_MAPPING__ != null) productThemeStyleMapping = window.__PRODUCT_THEME_STYLE_MAPPING__;
        else fetch(DATA_API_BASE + '/product_theme_style_mapping', { credentials: 'include' }).then(function (r) { return r.ok ? r.json() : null; }).catch(function () { return null; }).then(function (m) { productThemeStyleMapping = m && (m.byUnifiedId || m.byProductName) ? m : { byUnifiedId: {}, byProductName: {} }; });
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
    var dataUrl = DATA_API_BASE + '/weeks_index';
    var controller = typeof AbortController !== 'undefined' ? new AbortController() : null;
    var timeoutId = controller ? window.setTimeout(function () {
      controller.abort();
    }, 10000) : null;
    var opts = controller ? { signal: controller.signal } : {};
    fetch(dataUrl, { credentials: 'include', ...opts })
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
          else fetch(DATA_API_BASE + '/product_theme_style_mapping', { credentials: 'include' }).then(function (r) { return r.ok ? r.json() : null; }).catch(function () { return null; }).then(function (m) { productThemeStyleMapping = m && (m.byUnifiedId || m.byProductName) ? m : { byUnifiedId: {}, byProductName: {} }; });
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

  function downloadMonitorTable() {
    if (!currentYear || !currentWeek) return;
    var url = '/api/maintenance/download?year=' + encodeURIComponent(currentYear) + '&week=' + encodeURIComponent(currentWeek);
    var a = document.createElement('a');
    a.href = url;
    a.download = (currentWeek || 'table') + '_SLG数据监测表.xlsx';
    a.click();
  }

  if (searchInput) searchInput.addEventListener('input', filterRows);
  if (productSearchInput) productSearchInput.addEventListener('input', filterRows);
  if (productNewGameSearch) productNewGameSearch.addEventListener('input', function () {
    if (!newProductsWeekFilteredRows) return;
    var q = (productNewGameSearch.value || '').trim().toLowerCase();
    if (!q) {
      newProductsFilteredRows = newProductsWeekFilteredRows.slice();
    } else {
      newProductsFilteredRows = newProductsWeekFilteredRows.filter(function (row) {
        return row.some(function (cell) { return String(cell || '').toLowerCase().indexOf(q) >= 0; });
      });
    }
    renderNewProductsTable();
    if (productNewGameEmpty && productNewGameTableWrap) {
      if (!newProductsFilteredRows || newProductsFilteredRows.length === 0) {
        show(productNewGameEmpty);
        hide(productNewGameTableWrap);
      } else {
        hide(productNewGameEmpty);
        show(productNewGameTableWrap);
      }
    }
  });
  if (btnDownload) btnDownload.addEventListener('click', downloadTable);
  if (btnDownloadMonitorTable) btnDownloadMonitorTable.addEventListener('click', downloadMonitorTable);
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
    var start = e.target && e.target.nodeType === 1 ? e.target : (e.target && e.target.parentElement);
    var companyLink = start && start.closest ? start.closest('a.company-link') : null;
    if (companyLink) {
      e.preventDefault();
      openCompanyDetail(companyLink.textContent.trim());
      return;
    }
    var link = start && start.closest ? start.closest('a.product-link') : null;
    if (link) {
      e.preventDefault();
      var productName = link.textContent.trim();
      var tr = start && start.closest ? start.closest('tr') : null;
      var unifiedId = (tr && tr.dataset && tr.dataset.unifiedId) ? tr.dataset.unifiedId : null;
      var info = { name: productName, key: (currentDimension === 'company' ? 'old' : (productSelect.value || 'old')), unifiedId: unifiedId };
      if (currentDimension === 'product') openProductDetail(info, 'product');
      else if (currentDimension === 'company') openProductDetail(info, 'company');
      else openCreative(productName);
      return;
    }
    var tr = start && start.closest ? start.closest('tr') : null;
    if (tr && tr.classList.contains('target-product-row') && tr.dataset.product) {
      e.preventDefault();
      openCreative(tr.dataset.product);
    }
  });

  document.querySelectorAll('.top-nav-link').forEach(link => {
    link.addEventListener('click', function (e) {
      e.preventDefault();
      goToDimension(this.dataset.dim);
    });
  });

  // 唯一驱动：浏览器前进/后退或程序 setRoute 后由 hash 驱动；仅当维度变化时应用，避免重复执行
  window.addEventListener('hashchange', function () {
    var dim = parseHash();
    if (currentDimension !== dim) applyRoute(dim);
  });

  // 公司详情页「公司产品汇总」表中产品名点击 → 跳转该产品最新周详情（当前查看周即最新周）
  var companyDetailTableWrap = document.querySelector('.company-product-table-wrap');
  if (companyDetailTableWrap) {
    companyDetailTableWrap.addEventListener('click', function (e) {
      var link = e.target && e.target.closest && e.target.closest('a.company-detail-product-link');
      if (!link) return;
      e.preventDefault();
      var productName = (link.dataset && link.dataset.productName) ? link.dataset.productName : (link.textContent || '').trim();
      var unifiedId = (link.dataset && link.dataset.unifiedId) ? link.dataset.unifiedId : null;
      openProductDetail({ name: productName, key: 'old', unifiedId: unifiedId || undefined }, 'company');
    });
  }

  // 公司产品汇总表：点击表头列排序
  var companyProductTable = document.querySelector('.company-product-table');
  if (companyProductTable && companyProductTable.tHead) {
    companyProductTable.tHead.addEventListener('click', function (e) {
      var th = e.target && e.target.closest && e.target.closest('th.sortable');
      if (!th || th.dataset.col == null) return;
      var col = parseInt(th.dataset.col, 10);
      if (companyDetailProductSortCol === col) companyDetailProductSortAsc = !companyDetailProductSortAsc;
      else { companyDetailProductSortCol = col; companyDetailProductSortAsc = true; }
      renderCompanyDetailProductTable();
    });
  }

  var companyDetailSubNavEl = document.getElementById('companyDetailSubNav');
  if (companyDetailSubNavEl) {
    companyDetailSubNavEl.querySelectorAll('.company-detail-sub-link').forEach(function (link) {
      link.addEventListener('click', function (e) {
        e.preventDefault();
        var tab = this.dataset.tab;
        companyDetailSubNavEl.querySelectorAll('.company-detail-sub-link').forEach(function (l) { l.classList.remove('active'); });
        this.classList.add('active');
        if (tab === 'market') goToDimension('company');
      });
    });
  }

  // 产品详细信息页：大盘数据/上线新游 统一跳转当周产品维度首页；详细数据留在当前页
  var productDetailSubNav = document.getElementById('productDetailSubNav');
  if (productDetailSubNav) {
    productDetailSubNav.querySelectorAll('.product-detail-sub-link').forEach(function (link) {
      link.addEventListener('click', function (e) {
        e.preventDefault();
        var tab = this.dataset.tab;
        productDetailSubNav.querySelectorAll('.product-detail-sub-link').forEach(function (l) { l.classList.remove('active'); });
        this.classList.add('active');
        if (tab === 'detail') return;
        if (tab === 'market') {
          currentProductSubTab = 'overall';
          if (productSelect) productSelect.value = 'old';
          goToDimension('product');
          return;
        }
        if (tab === 'new') {
          currentProductSubTab = 'new';
          if (productSelect) productSelect.value = 'new';
          goToDimension('product');
        }
      });
    });
  }

  var btnProductDetailToCreative = document.getElementById('btnProductDetailToCreative');
  if (btnProductDetailToCreative) {
    btnProductDetailToCreative.addEventListener('click', function () {
      if (selectedProductForDetail && selectedProductForDetail.name) {
        openCreative(selectedProductForDetail.name, selectedProductForDetail.key);
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

  // 产品维度子导航：大盘数据 | 上线新游 | 详细数据；上线新游仅搜索+空状态/表格
  var productHomeSubNav = document.getElementById('productHomeSubNav');
  if (productHomeSubNav) {
    productHomeSubNav.querySelectorAll('.company-detail-sub-link').forEach(function (link) {
      link.addEventListener('click', function (e) {
        e.preventDefault();
        var tab = this.dataset.tab || 'overall';
        if (tab !== 'overall' && tab !== 'new' && tab !== 'detail') return;
        currentProductSubTab = tab;
        productHomeSubNav.querySelectorAll('.company-detail-sub-link').forEach(function (l) { l.classList.remove('active'); });
        this.classList.add('active');
        if (currentDimension !== 'product') return;
        if (tab === 'new') {
          showProductSubTabContent();
        } else if (tab === 'overall') {
          if (productSelect) productSelect.value = 'old';
          showProductSubTabContent();
          if (currentYear && currentWeek) loadProductWeek(currentYear, currentWeek);
        } else {
          showProductSubTabContent();
        }
      });
    });
  }

  // 数据底表子导航：产品总表 | 产品归属表 | 公司归属表 | 新产品监测表 | 题材/玩法/画风标签表；仅产品总表时显示侧边栏并高亮所选周
  if (basetableSubNav) {
    basetableSubNav.querySelectorAll('.company-detail-sub-link').forEach(function (link) {
      link.addEventListener('click', function (e) {
        e.preventDefault();
        var tab = this.getAttribute('data-tab');
        if (!tab) return;
        currentBasetableTab = tab;
        basetableSubNav.querySelectorAll('.company-detail-sub-link').forEach(function (l) { l.classList.remove('active'); });
        this.classList.add('active');
        var layoutEl = document.getElementById('layout');
        if (layoutEl) layoutEl.classList.toggle('layout-basetable-no-sidebar', tab !== 'metrics_total');
        if (currentDimension === 'basetable') {
          loadBasetableContent(tab);
          if (tab === 'metrics_total' && currentYear && currentWeek && typeof setActiveWeekInSidebar === 'function') setActiveWeekInSidebar(currentYear, currentWeek);
        }
      });
    });
  }
  // 产品总表搜索：防抖 400ms 或 Enter 触发重新加载
  var basetableMetricsSearchDebounce = null;
  if (basetableMetricsSearch) {
    function doBasetableMetricsSearch() {
      if (currentDimension === 'basetable' && currentBasetableTab === 'metrics_total' && currentYear && currentWeek) {
        loadBasetableContent('metrics_total');
      }
    }
    basetableMetricsSearch.addEventListener('input', function () {
      if (basetableMetricsSearchDebounce) clearTimeout(basetableMetricsSearchDebounce);
      basetableMetricsSearchDebounce = setTimeout(doBasetableMetricsSearch, 400);
    });
    basetableMetricsSearch.addEventListener('keydown', function (e) {
      if (e.key === 'Enter') {
        if (basetableMetricsSearchDebounce) clearTimeout(basetableMetricsSearchDebounce);
        basetableMetricsSearchDebounce = null;
        doBasetableMetricsSearch();
      }
    });
  }
  // 产品归属表 / 公司归属表 / 新产品监测表 / 题材·玩法·画风标签表：搜索栏即时筛选
  if (basetableOtherSearch) {
    basetableOtherSearch.addEventListener('input', function () {
      if (currentDimension === 'basetable' && basetableCachedData) renderBasetableOtherTable();
    });
  }

  // 数据维护：进度条（无依赖，请求期间显示动画 + 模拟百分比）
  function maintenanceProgressStart(wrapId, pctId) {
    var wrap = document.getElementById(wrapId);
    var pctEl = document.getElementById(pctId);
    if (!wrap || !pctEl) return function () {};
    wrap.classList.add('visible');
    wrap.setAttribute('aria-hidden', 'false');
    pctEl.textContent = '0%';
    var pct = 0;
    var tick = setInterval(function () {
      if (pct < 92) {
        pct += 2 + Math.floor(Math.random() * 4);
        if (pct > 92) pct = 92;
        pctEl.textContent = pct + '%';
      }
    }, 600);
    return function () {
      clearInterval(tick);
      pctEl.textContent = '100%';
      setTimeout(function () {
        wrap.classList.remove('visible');
        wrap.setAttribute('aria-hidden', 'true');
      }, 500);
    };
  }

  // 数据维护：第一步卡片 — 上传 13 个 CSV → 公司维度大盘数据（run_full_pipeline 第一步）
  var MAINTENANCE_PHASE1_URL = '/api/maintenance/phase1';
  var maintenancePhase1Form = document.getElementById('maintenancePhase1Form');
  var maintenancePhase1Files = document.getElementById('maintenancePhase1Files');
  var maintenancePhase1Submit = document.getElementById('maintenancePhase1Submit');
  var maintenancePhase1Status = document.getElementById('maintenancePhase1Status');
  if (maintenancePhase1Form && maintenancePhase1Files && maintenancePhase1Status) {
    maintenancePhase1Form.addEventListener('submit', function (e) {
      e.preventDefault();
      var yearVal = (document.getElementById('maintenanceYear') && document.getElementById('maintenanceYear').value) || '';
      var weekVal = (document.getElementById('maintenanceWeek') && document.getElementById('maintenanceWeek').value) || '';
      if (!yearVal.trim() || !weekVal.trim()) {
        maintenancePhase1Status.textContent = '请填写年份与周标签';
        maintenancePhase1Status.className = 'maintenance-status-inline err';
        return;
      }
      var files = maintenancePhase1Files.files;
      if (!files || files.length === 0) {
        maintenancePhase1Status.textContent = '请选择至少一个 CSV 文件';
        maintenancePhase1Status.className = 'maintenance-status-inline err';
        return;
      }
      var formData = new FormData();
      formData.append('year', yearVal.trim());
      formData.append('week_tag', weekVal.trim());
      for (var i = 0; i < files.length; i++) formData.append('files', files[i], (files[i].name && files[i].name.toLowerCase().endsWith('.csv')) ? files[i].name : 'file' + i + '.csv');
      if (maintenancePhase1Submit) {
        maintenancePhase1Submit.disabled = true;
        maintenancePhase1Submit.textContent = '上传并执行中…';
      }
      maintenancePhase1Status.textContent = '正在上传并执行第一步流水线…';
      maintenancePhase1Status.className = 'maintenance-status-inline';
      var endProgress = maintenanceProgressStart('maintenancePhase1Progress', 'maintenancePhase1ProgressPct');
      fetch(MAINTENANCE_PHASE1_URL, { method: 'POST', body: formData })
        .then(function (r) {
          if (!r.ok) throw new Error(r.statusText || '请求失败');
          return r.json().catch(function () { return {}; });
        })
        .then(function (data) {
          maintenancePhase1Status.textContent = data.message || '第一步执行完成，公司维度大盘数据已更新。可刷新页面或切换周期查看。';
          maintenancePhase1Status.className = 'maintenance-status-inline ok';
          if (typeof loadWeeksIndex === 'function') loadWeeksIndex();
        })
        .catch(function (err) {
          maintenancePhase1Status.textContent = err.message || '请求失败，请确认后端已启动且路径正确（' + MAINTENANCE_PHASE1_URL + '）';
          maintenancePhase1Status.className = 'maintenance-status-inline err';
        })
        .finally(function () {
          endProgress();
          if (maintenancePhase1Submit) {
            maintenancePhase1Submit.disabled = false;
            maintenancePhase1Submit.textContent = '上传并执行第一步';
          }
        });
    });
  }

  // 数据维护：2.1 步卡片 — 拉取目标产品分地区数据
  var MAINTENANCE_PHASE2_1_URL = '/api/maintenance/phase2_1';
  var maintenancePhase2_1Form = document.getElementById('maintenancePhase2_1Form');
  var maintenancePhase2_1Submit = document.getElementById('maintenancePhase2_1Submit');
  var maintenancePhase2_1Status = document.getElementById('maintenancePhase2_1Status');
  var MAINTENANCE_API_CONFIRM_MSG = '⚠️ API 限额为 3000 次/每月，请谨慎使用！\n\n确认要执行本次拉取吗？';
  if (maintenancePhase2_1Form && maintenancePhase2_1Status) {
    maintenancePhase2_1Form.addEventListener('submit', function (e) {
      e.preventDefault();
      var yearEl = document.getElementById('maintenancePhase2_1Year');
      var weekEl = document.getElementById('maintenancePhase2_1Week');
      var targetEl = document.getElementById('maintenancePhase2_1Target');
      var productTypeEl = document.getElementById('maintenancePhase2_1ProductType');
      var limitEl = document.getElementById('maintenancePhase2_1Limit');
      var unifiedIdEl = document.getElementById('maintenancePhase2_1UnifiedId');
      var yearVal = (yearEl && yearEl.value) ? yearEl.value.trim() : '';
      var weekVal = (weekEl && weekEl.value) ? weekEl.value.trim() : '';
      var unifiedIdVal = (unifiedIdEl && unifiedIdEl.value) ? unifiedIdEl.value.trim() : '';
      var target = (targetEl && targetEl.value) ? targetEl.value.trim() : '';
      var productType = (productTypeEl && productTypeEl.value) ? productTypeEl.value.trim() : '';
      var limit = (limitEl && limitEl.value) ? limitEl.value.trim() : '';
      if (!yearVal || !weekVal) {
        maintenancePhase2_1Status.textContent = '请填写年份与周标签';
        maintenancePhase2_1Status.className = 'maintenance-status-inline err';
        return;
      }
      if (!unifiedIdVal && (!target || !productType || !limit)) {
        maintenancePhase2_1Status.textContent = '请填写 Unified ID 单产品拉取，或选择目标产品、新/老产品与拉取数量';
        maintenancePhase2_1Status.className = 'maintenance-status-inline err';
        return;
      }
      if (!window.confirm('【2.1 步】' + MAINTENANCE_API_CONFIRM_MSG)) {
        return;
      }
      if (maintenancePhase2_1Submit) {
        maintenancePhase2_1Submit.disabled = true;
        maintenancePhase2_1Submit.textContent = '拉取中…';
      }
      maintenancePhase2_1Status.textContent = '正在执行 2.1 步拉取地区数据…';
      maintenancePhase2_1Status.className = 'maintenance-status-inline';
      var body = { year: yearVal, week_tag: weekVal, target: unifiedIdVal ? 'strategy' : target, product_type: unifiedIdVal ? 'both' : productType, limit: unifiedIdVal ? 'all' : limit };
      if (unifiedIdVal) body.unified_id = unifiedIdVal;
      var endProgress2_1 = maintenanceProgressStart('maintenancePhase2_1Progress', 'maintenancePhase2_1ProgressPct');
      fetch(MAINTENANCE_PHASE2_1_URL, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(body)
      })
        .then(function (r) {
          if (!r.ok) throw new Error(r.statusText || '请求失败');
          return r.json().catch(function () { return {}; });
        })
        .then(function (data) {
          maintenancePhase2_1Status.textContent = data.message || '2.1 步执行完成，分地区数据已更新。';
          maintenancePhase2_1Status.className = 'maintenance-status-inline ok';
          if (typeof loadWeeksIndex === 'function') loadWeeksIndex();
        })
        .catch(function (err) {
          maintenancePhase2_1Status.textContent = err.message || '请求失败，请确认后端已启动（' + MAINTENANCE_PHASE2_1_URL + '）';
          maintenancePhase2_1Status.className = 'maintenance-status-inline err';
        })
        .finally(function () {
          endProgress2_1();
          if (maintenancePhase2_1Submit) {
            maintenancePhase2_1Submit.disabled = false;
            maintenancePhase2_1Submit.textContent = '拉取并执行 2.1 步';
          }
        });
    });
  }

  // 数据维护：2.2 步卡片 — 拉取目标产品创意数据
  var MAINTENANCE_PHASE2_2_URL = '/api/maintenance/phase2_2';
  var maintenancePhase2_2Form = document.getElementById('maintenancePhase2_2Form');
  var maintenancePhase2_2Submit = document.getElementById('maintenancePhase2_2Submit');
  var maintenancePhase2_2Status = document.getElementById('maintenancePhase2_2Status');
  if (maintenancePhase2_2Form && maintenancePhase2_2Status) {
    maintenancePhase2_2Form.addEventListener('submit', function (e) {
      e.preventDefault();
      var yearEl = document.getElementById('maintenancePhase2_2Year');
      var weekEl = document.getElementById('maintenancePhase2_2Week');
      var targetEl = document.getElementById('maintenancePhase2_2Target');
      var productTypeEl = document.getElementById('maintenancePhase2_2ProductType');
      var limitEl = document.getElementById('maintenancePhase2_2Limit');
      var unifiedIdEl = document.getElementById('maintenancePhase2_2UnifiedId');
      var yearVal = (yearEl && yearEl.value) ? yearEl.value.trim() : '';
      var weekVal = (weekEl && weekEl.value) ? weekEl.value.trim() : '';
      var unifiedIdVal = (unifiedIdEl && unifiedIdEl.value) ? unifiedIdEl.value.trim() : '';
      var target = (targetEl && targetEl.value) ? targetEl.value.trim() : '';
      var productType = (productTypeEl && productTypeEl.value) ? productTypeEl.value.trim() : '';
      var limit = (limitEl && limitEl.value) ? limitEl.value.trim() : '';
      if (!yearVal || !weekVal) {
        maintenancePhase2_2Status.textContent = '请填写年份与周标签';
        maintenancePhase2_2Status.className = 'maintenance-status-inline err';
        return;
      }
      if (!unifiedIdVal && (!target || !productType || !limit)) {
        maintenancePhase2_2Status.textContent = '请填写 Unified ID 单产品拉取，或选择目标产品、新/老产品与拉取数量';
        maintenancePhase2_2Status.className = 'maintenance-status-inline err';
        return;
      }
      if (!window.confirm('【2.2 步】' + MAINTENANCE_API_CONFIRM_MSG)) {
        return;
      }
      if (maintenancePhase2_2Submit) {
        maintenancePhase2_2Submit.disabled = true;
        maintenancePhase2_2Submit.textContent = '拉取中…';
      }
      maintenancePhase2_2Status.textContent = '正在执行 2.2 步拉取创意数据…';
      maintenancePhase2_2Status.className = 'maintenance-status-inline';
      var body = { year: yearVal, week_tag: weekVal, target: unifiedIdVal ? 'strategy' : target, product_type: unifiedIdVal ? 'both' : productType, limit: unifiedIdVal ? 'all' : limit };
      if (unifiedIdVal) body.unified_id = unifiedIdVal;
      var endProgress2_2 = maintenanceProgressStart('maintenancePhase2_2Progress', 'maintenancePhase2_2ProgressPct');
      fetch(MAINTENANCE_PHASE2_2_URL, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(body)
      })
        .then(function (r) {
          if (!r.ok) throw new Error(r.statusText || '请求失败');
          return r.json().catch(function () { return {}; });
        })
        .then(function (data) {
          maintenancePhase2_2Status.textContent = data.message || '2.2 步执行完成，创意数据已更新。';
          maintenancePhase2_2Status.className = 'maintenance-status-inline ok';
          if (typeof loadWeeksIndex === 'function') loadWeeksIndex();
        })
        .catch(function (err) {
          maintenancePhase2_2Status.textContent = err.message || '请求失败，请确认后端已启动（' + MAINTENANCE_PHASE2_2_URL + '）';
          maintenancePhase2_2Status.className = 'maintenance-status-inline err';
        })
        .finally(function () {
          endProgress2_2();
          if (maintenancePhase2_2Submit) {
            maintenancePhase2_2Submit.disabled = false;
            maintenancePhase2_2Submit.textContent = '拉取并执行 2.2 步';
          }
        });
    });
  }

  // 数据维护：产品/公司归属表更新卡片
  var MAINTENANCE_MAPPING_UPDATE_URL = '/api/maintenance/mapping_update';
  var maintenanceMappingUpdateForm = document.getElementById('maintenanceMappingUpdateForm');
  var maintenanceMappingUpdateFile = document.getElementById('maintenanceMappingUpdateFile');
  var maintenanceMappingUpdateSubmit = document.getElementById('maintenanceMappingUpdateSubmit');
  var maintenanceMappingUpdateStatus = document.getElementById('maintenanceMappingUpdateStatus');
  if (maintenanceMappingUpdateForm && maintenanceMappingUpdateFile && maintenanceMappingUpdateStatus) {
    maintenanceMappingUpdateForm.addEventListener('submit', function (e) {
      e.preventDefault();
      var file = maintenanceMappingUpdateFile.files && maintenanceMappingUpdateFile.files[0];
      if (!file) {
        maintenanceMappingUpdateStatus.textContent = '请选择一个 .xlsx 文件';
        maintenanceMappingUpdateStatus.className = 'maintenance-status-inline err';
        return;
      }
      if (!file.name.toLowerCase().endsWith('.xlsx')) {
        maintenanceMappingUpdateStatus.textContent = '请上传 .xlsx 格式的 Excel 文件';
        maintenanceMappingUpdateStatus.className = 'maintenance-status-inline err';
        return;
      }
      var formData = new FormData();
      formData.append('file', file, file.name);
      if (maintenanceMappingUpdateSubmit) {
        maintenanceMappingUpdateSubmit.disabled = true;
        maintenanceMappingUpdateSubmit.textContent = '上传并更新中…';
      }
      maintenanceMappingUpdateStatus.textContent = '正在上传并更新归属表…';
      maintenanceMappingUpdateStatus.className = 'maintenance-status-inline';
      var endProgressMapping = maintenanceProgressStart('maintenanceMappingUpdateProgress', 'maintenanceMappingUpdateProgressPct');
      fetch(MAINTENANCE_MAPPING_UPDATE_URL, { method: 'POST', body: formData })
        .then(function (r) {
          if (!r.ok) throw new Error(r.statusText || '请求失败');
          return r.json().catch(function () { return {}; });
        })
        .then(function (data) {
          maintenanceMappingUpdateStatus.textContent = data.message || '归属表已更新。';
          maintenanceMappingUpdateStatus.className = data.ok ? 'maintenance-status-inline ok' : 'maintenance-status-inline err';
        })
        .catch(function (err) {
          maintenanceMappingUpdateStatus.textContent = err.message || '请求失败，请确认后端已启动（' + MAINTENANCE_MAPPING_UPDATE_URL + '）';
          maintenanceMappingUpdateStatus.className = 'maintenance-status-inline err';
        })
        .finally(function () {
          endProgressMapping();
          if (maintenanceMappingUpdateSubmit) {
            maintenanceMappingUpdateSubmit.disabled = false;
            maintenanceMappingUpdateSubmit.textContent = '上传并更新归属表';
          }
        });
    });
  }

  // 数据维护：新产品监测表卡片
  var MAINTENANCE_NEWPRODUCTS_UPDATE_URL = '/api/maintenance/newproducts_update';
  var maintenanceNewProductsForm = document.getElementById('maintenanceNewProductsForm');
  var maintenanceNewProductsFile = document.getElementById('maintenanceNewProductsFile');
  var maintenanceNewProductsSubmit = document.getElementById('maintenanceNewProductsSubmit');
  var maintenanceNewProductsStatus = document.getElementById('maintenanceNewProductsStatus');
  if (maintenanceNewProductsForm && maintenanceNewProductsFile && maintenanceNewProductsStatus) {
    maintenanceNewProductsForm.addEventListener('submit', function (e) {
      e.preventDefault();
      var file = maintenanceNewProductsFile.files && maintenanceNewProductsFile.files[0];
      if (!file) {
        maintenanceNewProductsStatus.textContent = '请选择一个 .xlsx 文件';
        maintenanceNewProductsStatus.className = 'maintenance-status-inline err';
        return;
      }
      if (!file.name.toLowerCase().endsWith('.xlsx')) {
        maintenanceNewProductsStatus.textContent = '请上传 .xlsx 格式的 Excel 文件';
        maintenanceNewProductsStatus.className = 'maintenance-status-inline err';
        return;
      }
      var formData = new FormData();
      formData.append('file', file, file.name);
      if (maintenanceNewProductsSubmit) {
        maintenanceNewProductsSubmit.disabled = true;
        maintenanceNewProductsSubmit.textContent = '上传并更新中…';
      }
      maintenanceNewProductsStatus.textContent = '正在上传新产品监测表…';
      maintenanceNewProductsStatus.className = 'maintenance-status-inline';
      var endProgressNewProducts = maintenanceProgressStart('maintenanceNewProductsProgress', 'maintenanceNewProductsProgressPct');
      fetch(MAINTENANCE_NEWPRODUCTS_UPDATE_URL, { method: 'POST', body: formData })
        .then(function (r) {
          if (!r.ok) throw new Error(r.statusText || '请求失败');
          return r.json().catch(function () { return {}; });
        })
        .then(function (data) {
          maintenanceNewProductsStatus.textContent = data.message || '新产品监测表已更新。';
          maintenanceNewProductsStatus.className = data.ok ? 'maintenance-status-inline ok' : 'maintenance-status-inline err';
          if (data.ok && typeof loadNewProducts === 'function') loadNewProducts();
        })
        .catch(function (err) {
          maintenanceNewProductsStatus.textContent = err.message || '请求失败，请确认后端已启动（' + MAINTENANCE_NEWPRODUCTS_UPDATE_URL + '）';
          maintenanceNewProductsStatus.className = 'maintenance-status-inline err';
        })
        .finally(function () {
          endProgressNewProducts();
          if (maintenanceNewProductsSubmit) {
            maintenanceNewProductsSubmit.disabled = false;
            maintenanceNewProductsSubmit.textContent = '上传并更新新产品监测表';
          }
        });
    });
  }

  function showLoginOverlay() {
    if (loginOverlay) loginOverlay.style.display = 'flex';
    if (appWrap) appWrap.style.display = 'none';
  }
  function hideLoginOverlay() {
    if (loginOverlay) loginOverlay.style.display = 'none';
    if (appWrap) appWrap.style.display = '';
  }
  function setHeaderUsername(name) {
    if (headerUsername) headerUsername.textContent = name ? name + ' · ' : '';
  }

  /** forceCompany: true 表示刚登录，跳转公司维度首页；false 表示刷新/已登录，保留当前 URL hash 页面 */
  function runApp(forceCompany) {
    if (forceCompany) setRoute('company');
    hideLoginOverlay();
    loadWeeksIndex();
  }

  function updateNavByRole() {
    var isSuperAdmin = currentUserRole === 'super_admin';
    document.querySelectorAll('.nav-admin-only').forEach(function (el) {
      el.style.display = isSuperAdmin ? '' : 'none';
    });
  }

  function checkAuthThenInit() {
    fetch('/api/auth/check', { credentials: 'include' })
      .then(function (r) { return r.status === 200 ? r.json() : Promise.reject(); })
      .then(function (data) {
        if (data && data.ok && data.username) {
          currentUserRole = (data.role || 'user') + '';
          // 兜底：若接口未返回 super_admin 但用户名为 admin 或 super_admin_*，仍按超级管理员显示入口
          var name = (data.username || '').trim();
          if (currentUserRole !== 'super_admin' && (name === 'admin' || name.indexOf('super_admin_') === 0)) {
            currentUserRole = 'super_admin';
          }
          setHeaderUsername(data.username);
          updateNavByRole();
          runApp(false);
        } else {
          showLoginOverlay();
        }
      })
      .catch(function () {
        showLoginOverlay();
      });
  }

  function bindLoginForm() {
    if (!loginForm) return;
    loginForm.addEventListener('submit', function (e) {
      e.preventDefault();
      var user = (loginUsername && loginUsername.value) ? loginUsername.value.trim() : '';
      var pass = (loginPassword && loginPassword.value) || '';
      if (loginError) loginError.textContent = '';
      if (!user) { if (loginError) loginError.textContent = '请输入用户名'; return; }
      if (!pass) { if (loginError) loginError.textContent = '请输入密码'; return; }
      if (loginSubmit) { loginSubmit.disabled = true; loginSubmit.textContent = '登录中…'; }
      fetch('/api/auth/login', {
        method: 'POST',
        credentials: 'include',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ username: user, password: pass })
      })
        .then(function (r) { return r.json(); })
        .then(function (data) {
          if (data && data.ok) {
            currentUserRole = (data.role || 'user') + '';
            var name = (data.username || '').trim();
            if (currentUserRole !== 'super_admin' && (name === 'admin' || name.indexOf('super_admin_') === 0)) {
              currentUserRole = 'super_admin';
            }
            setHeaderUsername(data.username);
            updateNavByRole();
            runApp(true);
          } else {
            if (loginError) loginError.textContent = (data && data.message) || '登录失败';
            if (loginSubmit) { loginSubmit.disabled = false; loginSubmit.textContent = '登录'; }
          }
        })
        .catch(function () {
          if (loginError) loginError.textContent = '网络错误，请重试';
          if (loginSubmit) { loginSubmit.disabled = false; loginSubmit.textContent = '登录'; }
        });
    });
  }

  function bindLogout() {
    if (!btnLogout) return;
    btnLogout.addEventListener('click', function () {
      fetch('/api/auth/logout', { method: 'POST', credentials: 'include' })
        .then(function () { window.location.reload(); })
        .catch(function () { window.location.reload(); });
    });
  }

  function switchToLoginTab() {
    var loginContent = document.getElementById('loginTabContent');
    var registerContent = document.getElementById('registerTabContent');
    var loginTabBtn = document.getElementById('loginTabBtn');
    var registerTabBtn = document.getElementById('registerTabBtn');
    if (loginContent) loginContent.style.display = '';
    if (registerContent) registerContent.style.display = 'none';
    if (loginTabBtn) loginTabBtn.classList.add('active');
    if (registerTabBtn) registerTabBtn.classList.remove('active');
  }

  function switchToRegisterTab() {
    var loginContent = document.getElementById('loginTabContent');
    var registerContent = document.getElementById('registerTabContent');
    var loginTabBtn = document.getElementById('loginTabBtn');
    var registerTabBtn = document.getElementById('registerTabBtn');
    if (loginContent) loginContent.style.display = 'none';
    if (registerContent) registerContent.style.display = '';
    if (loginTabBtn) loginTabBtn.classList.remove('active');
    if (registerTabBtn) registerTabBtn.classList.add('active');
  }

  function bindLoginRegisterTabs() {
    var loginTabBtn = document.getElementById('loginTabBtn');
    var registerTabBtn = document.getElementById('registerTabBtn');
    var goLoginLink = document.getElementById('goLoginLink');
    var goRegisterLink = document.getElementById('goRegisterLink');
    if (loginTabBtn) loginTabBtn.addEventListener('click', switchToLoginTab);
    if (registerTabBtn) registerTabBtn.addEventListener('click', switchToRegisterTab);
    if (goLoginLink) goLoginLink.addEventListener('click', function (e) { e.preventDefault(); switchToLoginTab(); });
    if (goRegisterLink) goRegisterLink.addEventListener('click', function (e) { e.preventDefault(); switchToRegisterTab(); });
  }

  function bindRegisterForm() {
    var registerForm = document.getElementById('registerForm');
    if (!registerForm) return;
    var registerUsername = document.getElementById('registerUsername');
    var registerPassword = document.getElementById('registerPassword');
    var registerPasswordConfirm = document.getElementById('registerPasswordConfirm');
    var registerError = document.getElementById('registerError');
    var registerSuccess = document.getElementById('registerSuccess');
    var registerSubmit = document.getElementById('registerSubmit');
    registerForm.addEventListener('submit', function (e) {
      e.preventDefault();
      var user = (registerUsername && registerUsername.value) ? registerUsername.value.trim() : '';
      var pass = (registerPassword && registerPassword.value) || '';
      var confirmPass = (registerPasswordConfirm && registerPasswordConfirm.value) || '';
      if (registerError) registerError.textContent = '';
      if (registerSuccess) { registerSuccess.style.display = 'none'; registerSuccess.textContent = ''; }
      if (!user) { if (registerError) registerError.textContent = '请输入用户名'; return; }
      if (user.length < 2) { if (registerError) registerError.textContent = '用户名至少 2 个字符'; return; }
      if (!pass || pass.length < 6) { if (registerError) registerError.textContent = '密码至少 6 位'; return; }
      if (pass !== confirmPass) { if (registerError) registerError.textContent = '两次密码不一致'; return; }
      if (registerSubmit) { registerSubmit.disabled = true; registerSubmit.textContent = '注册中…'; }
      fetch('/api/auth/register', {
        method: 'POST',
        credentials: 'include',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ username: user, password: pass })
      })
        .then(function (r) { return r.json(); })
        .then(function (data) {
          if (data && data.ok) {
            if (registerSuccess) {
              registerSuccess.textContent = (data.message || '注册成功，请等待管理员审批通过后登录');
              registerSuccess.style.display = '';
            }
            if (registerForm) registerForm.reset();
          } else {
            if (registerError) registerError.textContent = (data && data.message) || '注册失败';
          }
          if (registerSubmit) { registerSubmit.disabled = false; registerSubmit.textContent = '注册'; }
        })
        .catch(function () {
          if (registerError) registerError.textContent = '网络错误，请重试';
          if (registerSubmit) { registerSubmit.disabled = false; registerSubmit.textContent = '注册'; }
        });
    });
  }

  function renderAdvancedQueryResult(headers, rows) {
    advancedQueryLastResult = headers && rows ? { headers: headers, rows: rows } : null;
    if (!advancedQueryResultHead || !advancedQueryResultBody) return;
    advancedQueryResultHead.innerHTML = '';
    advancedQueryResultBody.innerHTML = '';
    if (!headers || !headers.length) {
      if (advancedQueryEmpty) { show(advancedQueryEmpty); advancedQueryEmpty.textContent = '无结果或执行非 SELECT 语句'; }
      if (advancedQueryResultWrap) hide(advancedQueryResultWrap);
      return;
    }
    var tr = document.createElement('tr');
    headers.forEach(function (h) {
      var th = document.createElement('th');
      th.textContent = h != null && h !== '' ? h : '—';
      tr.appendChild(th);
    });
    advancedQueryResultHead.appendChild(tr);
    (rows || []).forEach(function (row) {
      var tr = document.createElement('tr');
      headers.forEach(function (_, i) {
        var td = document.createElement('td');
        var v = row && row[i];
        td.textContent = v != null && v !== '' ? String(v) : '—';
        tr.appendChild(td);
      });
      advancedQueryResultBody.appendChild(tr);
    });
    if (advancedQueryEmpty) hide(advancedQueryEmpty);
    if (advancedQueryResultWrap) show(advancedQueryResultWrap);
  }

  function loadAdvancedQueryTables() {
    if (!advancedQueryTableList) return;
    advancedQueryTableList.innerHTML = '<li class="sidebar-month-title">加载中…</li>';
    fetch('/api/advanced_query/tables', { credentials: 'include' })
      .then(function (r) {
        return r.json().then(function (data) {
          if (!r.ok) throw new Error((data && data.message) || r.statusText || '请求失败');
          return data;
        }).catch(function (e) {
          if (e instanceof Error && e.message && e.message !== '请求失败') throw e;
          throw new Error(r.statusText || '请求失败');
        });
      })
      .then(function (data) {
        var tables = (data && data.tables) ? data.tables : [];
        advancedQueryTableList.innerHTML = '';
        tables.forEach(function (name) {
          var li = document.createElement('li');
          var a = document.createElement('a');
          a.href = '#';
          a.className = 'sidebar-table-link';
          a.dataset.table = name;
          a.textContent = name;
          li.appendChild(a);
          advancedQueryTableList.appendChild(li);
        });
        advancedQueryTableList.querySelectorAll('.sidebar-table-link').forEach(function (link) {
          link.addEventListener('click', function (e) {
            e.preventDefault();
            advancedQueryTableList.querySelectorAll('.sidebar-table-link').forEach(function (l) { l.classList.remove('active'); });
            this.classList.add('active');
            var tableName = this.dataset.table;
            if (!tableName) return;
            if (advancedQueryStatus) advancedQueryStatus.textContent = '加载中…';
            fetch('/api/advanced_query/table/' + encodeURIComponent(tableName), { credentials: 'include' })
              .then(function (r) {
                return r.text().then(function (text) {
                  if (!r.ok) {
                    var msg = '表不存在或服务器错误';
                    try {
                      var data = JSON.parse(text);
                      if (data && data.message) msg = data.message;
                    } catch (_) {
                      if (text && text.length < 200) msg = text; else if (text) msg = text.substring(0, 100) + '…';
                    }
                    return Promise.reject(new Error(msg));
                  }
                  try {
                    return JSON.parse(text);
                  } catch (e) {
                    return Promise.reject(new Error('服务器返回非 JSON，请检查后端是否正常'));
                  }
                });
              })
              .then(function (info) {
                if (advancedQueryStatus) advancedQueryStatus.textContent = '表「' + tableName + '」前 50 条';
                renderAdvancedQueryResult(info.headers || [], info.rows || []);
              })
              .catch(function (err) {
                if (advancedQueryStatus) advancedQueryStatus.textContent = err.message || '加载失败';
                renderAdvancedQueryResult([], []);
              });
          });
        });
      })
      .catch(function (err) {
        var msg = (err && err.message) ? err.message : '加载失败';
        var hint = '';
        if (msg.indexOf('高级查询需启用 MySQL') !== -1) {
          hint = '请按以下步骤操作：<br>1. 设置环境变量 USE_MYSQL=1<br>2. 启动 MySQL 并导入 backend/db/schema.sql<br>3. 启动时传入 MYSQL_USER、MYSQL_PASSWORD、MYSQL_DATABASE<br>4. 重启 start_server.py<br>详见 docs/高级查询启用MySQL步骤.md';
        } else {
          hint = msg;
        }
        advancedQueryTableList.innerHTML = '<li class="sidebar-month-title advanced-query-mysql-hint">' + hint + '</li>';
      });
  }

  function showApprovalView(tab) {
    currentApprovalTab = tab || 'pending';
    var viewPending = document.getElementById('approvalViewPending');
    var viewExisting = document.getElementById('approvalViewExisting');
    if (viewPending) viewPending.style.display = tab === 'pending' ? '' : 'none';
    if (viewExisting) viewExisting.style.display = tab === 'existing' ? '' : 'none';
    var links = document.querySelectorAll('.approval-tab-link');
    links.forEach(function (a) {
      if ((a.dataset.tab || '') === tab) a.classList.add('active'); else a.classList.remove('active');
    });
  }

  function loadApprovalExistingUsers() {
    var emptyEl = document.getElementById('approvalExistingEmpty');
    var tableEl = document.getElementById('approvalExistingTable');
    var bodyEl = document.getElementById('approvalExistingTableBody');
    if (!bodyEl) return;
    if (emptyEl) emptyEl.textContent = '加载中…';
    fetch('/api/auth/approved_users', { credentials: 'include' })
      .then(function (r) {
        if (r.status === 403) { if (emptyEl) emptyEl.textContent = '无权限'; return { ok: false, users: [] }; }
        if (r.status === 401) { if (emptyEl) emptyEl.textContent = '未登录或已过期，请重新登录'; return { ok: false, users: [] }; }
        if (!r.ok) {
          return r.text().then(function (text) {
            var msg = r.status + ' 请求失败';
            try { var d = JSON.parse(text); if (d && d.message) msg = d.message; } catch (_) { if (text && text.length < 80) msg = text; }
            if (emptyEl) emptyEl.textContent = msg;
            return { ok: false, users: [] };
          });
        }
        return r.json();
      })
      .then(function (data) {
        var users = (data && data.users) ? data.users : [];
        if (emptyEl) {
          emptyEl.style.display = users.length ? 'none' : '';
          emptyEl.textContent = users.length ? '' : '暂无用户';
        }
        if (tableEl) tableEl.style.display = users.length ? '' : 'none';
        bodyEl.innerHTML = '';
        users.forEach(function (u) {
          var username = (u.username || '').trim();
          if (!username) return;
          var tr = document.createElement('tr');
          var tdName = document.createElement('td');
          tdName.textContent = username;
          var tdRole = document.createElement('td');
          tdRole.textContent = (u.role || 'user') === 'super_admin' ? '超级管理员' : '普通用户';
          var tdStatus = document.createElement('td');
          tdStatus.textContent = (u.status || 'approved') === 'approved' ? '已通过' : (u.status || '—');
          tr.appendChild(tdName);
          tr.appendChild(tdRole);
          tr.appendChild(tdStatus);
          bodyEl.appendChild(tr);
        });
      })
      .catch(function (err) {
        if (emptyEl) emptyEl.textContent = '加载失败，请检查网络或后端是否启动';
      });
  }

  function loadApprovalPendingUsers() {
    var emptyEl = document.getElementById('approvalEmpty');
    var tableEl = document.getElementById('approvalTable');
    var bodyEl = document.getElementById('approvalTableBody');
    if (!bodyEl) return;
    fetch('/api/auth/pending_users', { credentials: 'include' })
      .then(function (r) {
        if (r.status === 403) { if (emptyEl) emptyEl.textContent = '无权限'; return { ok: false, users: [] }; }
        return r.json();
      })
      .then(function (data) {
        var users = (data && data.users) ? data.users : [];
        if (emptyEl) {
          emptyEl.style.display = users.length ? 'none' : '';
          emptyEl.textContent = users.length ? '' : '暂无待审批用户';
        }
        if (tableEl) tableEl.style.display = users.length ? '' : 'none';
        bodyEl.innerHTML = '';
        users.forEach(function (u) {
          var username = (u.username || '').trim();
          if (!username) return;
          var tr = document.createElement('tr');
          var tdName = document.createElement('td');
          tdName.textContent = username;
          var tdAction = document.createElement('td');
          var btn = document.createElement('button');
          btn.type = 'button';
          btn.className = 'btn-approve';
          btn.textContent = '通过';
          btn.dataset.username = username;
          btn.addEventListener('click', function () {
            btn.disabled = true;
            fetch('/api/auth/approve', {
              method: 'POST',
              credentials: 'include',
              headers: { 'Content-Type': 'application/json' },
              body: JSON.stringify({ username: username })
            })
              .then(function (res) { return res.json(); })
              .then(function (result) {
                if (result && result.ok) loadApprovalPendingUsers();
                else { btn.disabled = false; if (result && result.message) alert(result.message); }
              })
              .catch(function () { btn.disabled = false; });
          });
          tdAction.appendChild(btn);
          tr.appendChild(tdName);
          tr.appendChild(tdAction);
          bodyEl.appendChild(tr);
        });
      })
      .catch(function () {
        if (emptyEl) emptyEl.textContent = '加载失败';
      });
  }

  function bindApprovalPanel() {
    var refreshBtn = document.getElementById('approvalRefreshBtn');
    if (refreshBtn) refreshBtn.addEventListener('click', loadApprovalPendingUsers);
    var existingRefreshBtn = document.getElementById('approvalExistingRefreshBtn');
    if (existingRefreshBtn) existingRefreshBtn.addEventListener('click', loadApprovalExistingUsers);
    document.querySelectorAll('.approval-tab-link').forEach(function (link) {
      link.addEventListener('click', function (e) {
        e.preventDefault();
        var tab = (this.dataset.tab || 'pending').trim();
        showApprovalView(tab);
        if (tab === 'pending') loadApprovalPendingUsers();
        else if (tab === 'existing') loadApprovalExistingUsers();
      });
    });
  }

  function bindProductDetailSinglePull() {
    var btn2_1 = document.getElementById('productDetailPull2_1');
    var btn2_2 = document.getElementById('productDetailPull2_2');
    function doPull(stepLabel, url) {
      var year = currentYear ? String(currentYear).trim() : '';
      var week = currentWeek ? String(currentWeek).trim() : '';
      var uid = currentProductUnifiedId ? String(currentProductUnifiedId).trim() : '';
      if (!year || !week) {
        alert('请从左侧选择周期（年与周）后再执行拉取。');
        return;
      }
      if (!uid) {
        alert('当前产品无 Unified ID，无法执行单产品拉取。');
        return;
      }
      if (!window.confirm('【' + stepLabel + '】' + MAINTENANCE_API_CONFIRM_MSG + '\n将对该产品（Unified ID: ' + uid + '）及所选周期 ' + year + '年 ' + week + ' 执行一次拉取。')) return;
      var origText = (url === MAINTENANCE_PHASE2_1_URL ? btn2_1 : btn2_2).textContent;
      if (url === MAINTENANCE_PHASE2_1_URL && btn2_1) { btn2_1.disabled = true; btn2_1.textContent = '拉取中…'; }
      if (url === MAINTENANCE_PHASE2_2_URL && btn2_2) { btn2_2.disabled = true; btn2_2.textContent = '拉取中…'; }
      fetch(url, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ year: year, week_tag: week, unified_id: uid, target: 'strategy', product_type: 'both', limit: 'all' }),
        credentials: 'include'
      })
        .then(function (r) { return r.json().catch(function () { return {}; }); })
        .then(function (data) {
          alert(data.ok ? (data.message || '执行完成') : (data.message || '执行失败'));
          if (data.ok) {
            if (typeof loadWeeksIndex === 'function') loadWeeksIndex();
            if (currentDimension === 'product-detail' && typeof loadProductDetail === 'function') loadProductDetail();
          }
        })
        .catch(function (err) {
          alert('请求失败: ' + (err.message || err));
        })
        .finally(function () {
          if (btn2_1) { btn2_1.disabled = false; btn2_1.textContent = '拉取该产品 2.1 步'; }
          if (btn2_2) { btn2_2.disabled = false; btn2_2.textContent = '拉取该产品 2.2 步'; }
        });
    }
    if (btn2_1) btn2_1.addEventListener('click', function () { doPull('2.1 步', MAINTENANCE_PHASE2_1_URL); });
    if (btn2_2) btn2_2.addEventListener('click', function () { doPull('2.2 步', MAINTENANCE_PHASE2_2_URL); });
  }

  function bindProductDetailPinAndRefresh() {
    var refreshBtn = document.getElementById('productDetailRefreshBtn');
    if (refreshBtn) {
      refreshBtn.addEventListener('click', function () {
        if (typeof loadProductDetail === 'function') loadProductDetail();
      });
    }
  }

  function bindProductDetailUnifiedIdCopy() {
    var copyBtn = document.getElementById('productDetailUnifiedIdCopy');
    if (!copyBtn) return;
    copyBtn.addEventListener('click', function () {
      var uid = (this.dataset && this.dataset.unifiedId) ? this.dataset.unifiedId : '';
      if (!uid) {
        var valEl = document.getElementById('productDetailUnifiedId');
        uid = (valEl && valEl.textContent && valEl.textContent !== '—') ? valEl.textContent.trim() : '';
      }
      if (!uid) return;
      if (navigator.clipboard && navigator.clipboard.writeText) {
        navigator.clipboard.writeText(uid).then(function () {
          var t = copyBtn.textContent;
          copyBtn.textContent = '已复制';
          setTimeout(function () { copyBtn.textContent = t; }, 1500);
        }).catch(function () { alert('复制失败，请手动复制'); });
      } else {
        var ta = document.createElement('textarea');
        ta.value = uid;
        ta.style.position = 'fixed';
        ta.style.opacity = '0';
        document.body.appendChild(ta);
        ta.select();
        try {
          document.execCommand('copy');
          var t = copyBtn.textContent;
          copyBtn.textContent = '已复制';
          setTimeout(function () { copyBtn.textContent = t; }, 1500);
        } catch (e) { alert('复制失败，请手动复制'); }
        document.body.removeChild(ta);
      }
    });
  }

  function bindAdvancedQueryPanel() {
    if (advancedQueryRunBtn) {
      advancedQueryRunBtn.addEventListener('click', function () {
        var sql = (advancedQuerySql && advancedQuerySql.value) ? advancedQuerySql.value.trim() : '';
        if (!sql) {
          if (advancedQueryStatus) advancedQueryStatus.textContent = '请输入 SQL';
          return;
        }
        advancedQueryRunBtn.disabled = true;
        if (advancedQueryStatus) advancedQueryStatus.textContent = '执行中…';
        fetch('/api/advanced_query/execute', {
          method: 'POST',
          credentials: 'include',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ sql: sql })
        })
          .then(function (r) { return r.json(); })
          .then(function (res) {
            advancedQueryRunBtn.disabled = false;
            if (!res.ok) {
              if (advancedQueryStatus) advancedQueryStatus.textContent = res.message || '执行失败';
              renderAdvancedQueryResult([], []);
              return;
            }
            if (res.headers != null && res.rows != null) {
              if (advancedQueryStatus) advancedQueryStatus.textContent = '返回 ' + (res.rows.length || 0) + ' 行';
              renderAdvancedQueryResult(res.headers, res.rows);
            } else {
              if (advancedQueryStatus) advancedQueryStatus.textContent = '影响行数: ' + (res.affected != null ? res.affected : 0);
              renderAdvancedQueryResult([], []);
            }
          })
          .catch(function (err) {
            advancedQueryRunBtn.disabled = false;
            if (advancedQueryStatus) advancedQueryStatus.textContent = '请求失败';
            renderAdvancedQueryResult([], []);
          });
      });
    }
    if (advancedQueryDownloadBtn) {
      advancedQueryDownloadBtn.addEventListener('click', function () {
        if (!advancedQueryLastResult || !advancedQueryLastResult.headers || !advancedQueryLastResult.rows) {
          alert('当前无结果表可下载，请先执行 SELECT 或点击左侧表名查看数据。');
          return;
        }
        var headers = advancedQueryLastResult.headers;
        var rows = advancedQueryLastResult.rows;
        var csv = headers.map(function (h) { return '"' + String(h).replace(/"/g, '""') + '"'; }).join(',') + '\n';
        rows.forEach(function (row) {
          csv += headers.map(function (_, i) {
            var v = row && row[i];
            return '"' + String(v != null ? v : '').replace(/"/g, '""') + '"';
          }).join(',') + '\n';
        });
        var blob = new Blob(['\ufeff' + csv], { type: 'text/csv;charset=utf-8' });
        var url = URL.createObjectURL(blob);
        var a = document.createElement('a');
        a.href = url;
        a.download = 'advanced_query_result.csv';
        a.click();
        URL.revokeObjectURL(url);
      });
    }
  }

  // 等 DOM 完全就绪后先校验登录态，再加载数据或显示登录框
  function init() {
    bindLogout();
    checkAuthThenInit();
    bindLoginForm();
    bindLoginRegisterTabs();
    bindRegisterForm();
    bindApprovalPanel();
    bindProductDetailUnifiedIdCopy();
    bindProductDetailSinglePull();
    bindProductDetailPinAndRefresh();
    bindAdvancedQueryPanel();
  }
  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init);
  } else {
    init();
  }
})();
