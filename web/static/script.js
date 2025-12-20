// 全局变量
const g_columns = window.g_columns || [];
let g_templates = []; 

// === 分页与筛选状态 ===
let g_currentPage = 1;
const g_pageSize = 50;
let g_isLoading = false;
let g_hasMore = true;
let g_totalCount = 0;
let g_loadedData = []; 

// 查询条件
let g_queryState = {
    search: "",
    sortKey: "",
    sortDir: "asc",
    filters: {} 
};
const activeFilters = {}; 

document.addEventListener("DOMContentLoaded", function(){
    document.querySelectorAll('thead [data-bs-toggle="tooltip"]').forEach(el => {
        new bootstrap.Tooltip(el, {html: true});
    });

    const container = document.querySelector('.table-container');
    if (container) {
        container.addEventListener('scroll', function() {
            if (container.scrollTop + container.clientHeight >= container.scrollHeight - 200) {
                if (g_hasMore && !g_isLoading) {
                    loadData(false);
                }
            }
        });
    }

    loadData(true);
});

function escapeHtml(text) {
    if (!text) return "";
    return String(text).replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;").replace(/"/g, "&quot;").replace(/'/g, "&#039;");
}

// === 核心数据加载函数 ===
function loadData(isReset = false) {
    if (g_isLoading) return;
    g_isLoading = true;

    if (isReset) {
        g_currentPage = 1;
        g_hasMore = true;
        g_loadedData = [];
        document.getElementById('tableBody').innerHTML = ''; 
        document.querySelectorAll('.sort-icon').forEach(el => el.innerText = '');
        const th = document.querySelector(`th[data-key="${g_queryState.sortKey}"]`);
        if(th) {
            th.querySelector('.sort-icon').innerText = g_queryState.sortDir === 'asc' ? ' ▲' : ' ▼';
        }
        showLoading(true);
    } else {
        showLoading(false); 
        appendLoadingRow();
    }

    const payload = {
        page: g_currentPage,
        page_size: g_pageSize,
        sort_key: g_queryState.sortKey,
        sort_dir: g_queryState.sortDir,
        filters: g_queryState.filters,
        search: g_queryState.search
    };

    fetch('/api/stocks/query', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload)
    })
    .then(res => res.json())
    .then(data => {
        removeLoadingRow();
        
        g_totalCount = data.total;
        document.getElementById('visibleCount').innerText = g_totalCount;
        
        if (data.data.length > 0) {
            renderRows(data.data);
            g_loadedData = g_loadedData.concat(data.data);
            g_currentPage++;
        }
        
        if (data.data.length < g_pageSize) {
            g_hasMore = false;
            if (g_totalCount > 0) appendEndMessage();
        } 
        
        if (g_totalCount === 0 && isReset) {
             document.getElementById('tableBody').innerHTML = '<tr><td colspan="100" class="text-center py-4 text-muted">没有找到匹配的股票</td></tr>';
        }
    })
    .catch(err => {
        console.error(err);
        if(isReset) document.getElementById('tableBody').innerHTML = '<tr><td colspan="100" class="text-center py-4 text-danger">加载失败，请刷新重试</td></tr>';
    })
    .finally(() => {
        g_isLoading = false;
    });
}

function showLoading(isReset) {
    if(isReset) {
        const tbody = document.getElementById('tableBody');
        tbody.innerHTML = `<tr id="loadingSkeleton"><td colspan="100" class="text-center py-5"><div class="spinner-border text-primary mb-3"></div><h5 class="text-muted">正在加载数据...</h5></td></tr>`;
    }
}

function appendLoadingRow() {
    const tbody = document.getElementById('tableBody');
    if (!document.getElementById('loadingRow')) {
        tbody.insertAdjacentHTML('beforeend', `<tr id="loadingRow"><td colspan="100" class="text-center py-3"><div class="spinner-border spinner-border-sm text-primary"></div> 加载更多...</td></tr>`);
    }
}

function removeLoadingRow() {
    const row = document.getElementById('loadingRow');
    if(row) row.remove();
    const skel = document.getElementById('loadingSkeleton');
    if(skel) skel.remove();
}

function appendEndMessage() {
    const tbody = document.getElementById('tableBody');
    if (!document.getElementById('endMsgRow')) {
        tbody.insertAdjacentHTML('beforeend', `<tr id="endMsgRow"><td colspan="100" class="text-center py-2 text-muted small">-- 已显示全部数据 --</td></tr>`);
    }
}

// === 渲染逻辑 ===
function renderRows(batch) {
    const tbody = document.getElementById('tableBody');
    
    const rowsHtml = batch.map(stock => {
        let closePrice = 0;
        if (stock['昨收']) {
            closePrice = parseFloat(String(stock['昨收']).replace(/,/g, ''));
        }

        const colsHtml = g_columns.map(col => {
            let val = stock[col.key];
            if (val === undefined || val === null || val === '-' || val === '') {
                return `<td><span class="text-muted">-</span></td>`;
            }

            if (col.key === 'bull_label') {
                if (val.includes('5年')) return `<td><span class="badge bg-danger">👑 长牛5年</span></td>`;
                if (val.includes('4年')) return `<td><span class="badge bg-warning text-dark">🔥 长牛4年</span></td>`;
                if (val.includes('3年')) return `<td><span class="badge bg-primary">💎 长牛3年</span></td>`;
                if (val.includes('2年')) return `<td><span class="badge bg-info text-dark">⭐ 长牛2年</span></td>`;
                if (val.includes('1年')) return `<td><span class="badge bg-success">🌱 长牛1年</span></td>`;
                return `<td>${val}</td>`;
            }

            let num = parseFloat(String(val).replace(/,/g, ''));
            let displayVal = val; 
            let isNegative = false;
            let suffix = col.suffix || '';

            if (!isNaN(num)) {
                isNegative = num < 0;
                if (Math.abs(num) < 0.005) {
                    displayVal = "0.00";
                } else {
                    displayVal = num.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
                }
                displayVal += suffix;
            }

            if (isNegative) {
                displayVal = `<span class="text-danger">${displayVal}</span>`;
            }

            if (!isNaN(num)) {
                let badge = '';
                if (col.key === 'PEG') {
                    if (num >= 0 && num < 0.5) badge = `<span class="badge bg-success ms-1" style="font-size:10px; padding:2px 4px;">极低</span>`;
                    else if (num >= 0.5 && num <= 1) badge = `<span class="badge bg-info text-dark ms-1" style="font-size:10px; padding:2px 4px;">低</span>`;
                }
                else if (col.key === 'PEGY') {
                    if (num >= 0 && num <= 1) badge = `<span class="badge bg-info text-dark ms-1" style="font-size:10px; padding:2px 4px;">低</span>`;
                }
                else if ((col.key === '合理股价' || col.key === '格雷厄姆数') && closePrice > 0 && num > 0) {
                    let ratio = closePrice / num;
                    if (col.key === '合理股价') {
                        if (ratio < 0.5) badge = `<span class="badge bg-success ms-1" style="font-size:10px; padding:2px 4px;">极低</span>`;
                        else if (ratio >= 0.5 && ratio <= 0.67) badge = `<span class="badge bg-info text-dark ms-1" style="font-size:10px; padding:2px 4px;">低</span>`;
                    } else if (col.key === '格雷厄姆数') {
                        if (ratio < 0.7) badge = `<span class="badge bg-success ms-1" style="font-size:10px; padding:2px 4px;">极低</span>`;
                        else if (ratio >= 0.7 && ratio <= 0.9) badge = `<span class="badge bg-info text-dark ms-1" style="font-size:10px; padding:2px 4px;">低</span>`;
                    }
                }
                else if (col.key === '净现比' && num >= 1) {
                    badge = `<span class="badge bg-warning text-dark ms-1" style="font-size:10px; padding:2px 4px;">优</span>`;
                }

                if (badge) displayVal += badge;
            }
            
            if (col.no_chart) {
                return `<td>${displayVal}</td>`;
            } else {
                return `<td class="clickable-cell" onclick="loadChart('${stock.code}', '${col.key}', '${col.label}', '${suffix}')">${displayVal}</td>`;
            }
        }).join('');
        
        let nameContent = `<span class="text-truncate" style="max-width: 90px; display: inline-block; vertical-align: middle; cursor: default;" 
                                 data-bs-toggle="tooltip" data-bs-placement="top" title="${escapeHtml(stock.name)}">
                                 ${stock.name}
                           </span>`;
        if (stock.intro) {
            nameContent += `<span class="info-icon ms-1" style="vertical-align: middle; cursor: help;" data-bs-toggle="tooltip" data-bs-placement="right" title="${escapeHtml(stock.intro)}">?</span>`;
        }

        let codeColor = stock.is_ggt ? 'text-danger' : 'text-primary';
        let codeContent = `<a href="https://xueqiu.com/S/${stock.code}" target="_blank" class="${codeColor} stock-link fw-bold">${stock.code}</a>`;
        
        if (stock.is_ggt) {
            codeContent += `<span class="badge bg-primary ms-1 fw-normal" style="font-size: 10px; padding: 2px 4px; vertical-align: text-bottom;">通</span>`;
        }

        return `<tr>
            <td class="sticky-col col-code ps-2">${codeContent}</td>
            <td class="sticky-col col-name fw-bold d-flex align-items-center justify-content-between">${nameContent}</td>
            <td class="text-muted small">${stock.date}</td>
            ${colsHtml}
        </tr>`;
    }).join('');
    
    tbody.insertAdjacentHTML('beforeend', rowsHtml);

    tbody.querySelectorAll('[data-bs-toggle="tooltip"]').forEach(el => {
        if (!bootstrap.Tooltip.getInstance(el)) {
            new bootstrap.Tooltip(el, {html: true});
        }
    });
}

// === 筛选与排序 ===

function confirmFilter(btn, colKey) {
    const popup = btn.closest('.filter-popup');
    const minVal = popup.querySelector(`#min-${CSS.escape(colKey)}`).value;
    const maxVal = popup.querySelector(`#max-${CSS.escape(colKey)}`).value;
    
    if (!activeFilters[colKey]) activeFilters[colKey] = {};
    
    // [修改] 针对文本字段，不执行 parseFloat
    if (["所属行业", "bull_label"].includes(colKey)) {
        activeFilters[colKey].min = minVal; 
        activeFilters[colKey].max = null;   
    } else {
        activeFilters[colKey].min = minVal === "" ? null : parseFloat(minVal);
        activeFilters[colKey].max = maxVal === "" ? null : parseFloat(maxVal);
    }
    
    updateHeaderStyle(colKey);
    executeFiltering(); 
    
    popup.style.display = 'none';
    setTimeout(() => { popup.style.display = ''; }, 500);
}

function clearFilter(btn, colKey) {
    const popup = btn.closest('.filter-popup');
    popup.querySelector(`#min-${CSS.escape(colKey)}`).value = '';
    const maxInput = popup.querySelector(`#max-${CSS.escape(colKey)}`);
    if(maxInput) maxInput.value = '';
    
    if (activeFilters[colKey]) {
        activeFilters[colKey] = { min: null, max: null };
        updateHeaderStyle(colKey);
    }
    executeFiltering();
}

function updateHeaderStyle(colKey) {
    const th = document.querySelector(`th[data-key="${colKey}"]`);
    const filter = activeFilters[colKey];
    if (filter && (filter.min !== null || filter.max !== null)) th.classList.add('filter-active');
    else th.classList.remove('filter-active');
}

function executeFiltering() {
    const searchVal = document.getElementById('globalSearchInput').value.trim();
    g_queryState.search = searchVal;
    
    const cleanFilters = {};
    for (const [key, range] of Object.entries(activeFilters)) {
        if (range.min !== null || range.max !== null) {
            cleanFilters[key] = range;
        }
    }
    g_queryState.filters = cleanFilters;

    loadData(true);
}

function sortTable(key, type) {
    if (g_queryState.sortKey === key) {
        g_queryState.sortDir = g_queryState.sortDir === 'asc' ? 'desc' : 'asc';
    } else {
        g_queryState.sortKey = key;
        g_queryState.sortDir = 'asc';
    }
    loadData(true);
}

// === API 交互 ===
function triggerRecalculate() {
    if(!confirm("确定补全吗？")) return;
    document.getElementById('recalcBtn').disabled = true;
    fetch('/api/recalculate', { method: 'POST' });
}

function restartService() {
    if(!confirm("确定重启吗？")) return;
    fetch('/api/restart', { method: 'POST' }).then(() => {
        const mask = document.createElement('div');
        mask.style.cssText = "position:fixed;top:0;left:0;width:100%;height:100%;background:rgba(255,255,255,0.9);z-index:9999;display:flex;justify-content:center;align-items:center;flex-direction:column;";
        mask.innerHTML = `<div class="spinner-border text-primary mb-3" role="status"></div><h5 class="text-dark">服务正在重启...</h5><p class="text-muted">3秒后自动刷新</p>`;
        document.body.appendChild(mask);
        setTimeout(() => location.reload(), 3000);
    });
}

function triggerCrawl() {
    document.getElementById('refreshBtn').disabled = true;
    fetch('/api/trigger_crawl');
}

function stopTask() {
    if(!confirm("确定要终止当前任务吗？")) return;
    fetch('/api/stop_crawl', { method: 'POST' }).then(res => res.json()).then(data => {
        alert(data.message);
    });
}

setInterval(() => {
    fetch('/api/status').then(res => res.json()).then(data => {
        const container = document.getElementById('progress-container');
        const stopBtn = document.getElementById('stopBtn');
        const refreshBtn = document.getElementById('refreshBtn');
        const recalcBtn = document.getElementById('recalcBtn');

        if (data.is_running) {
            container.style.display = 'block';
            stopBtn.style.display = 'inline-block';
            refreshBtn.disabled = true;
            recalcBtn.disabled = true;
            
            const pct = data.total > 0 ? Math.round(data.current/data.total*100) : 0;
            document.getElementById('progress-bar').style.width = pct + "%";
            document.getElementById('progress-msg').innerText = `${data.message}`;
        } else {
            if (container.style.display === 'block') {
                loadData(true);
            }
            container.style.display = 'none';
            stopBtn.style.display = 'none';
            refreshBtn.disabled = false;
            recalcBtn.disabled = false;
        }
    });
}, 1500);

// === 图表与模态框 ===
var myChart = echarts.init(document.getElementById('chart-container'));
var chartModal = new bootstrap.Modal(document.getElementById('chartModal'));
document.getElementById('chartModal').addEventListener('shown.bs.modal', () => myChart.resize());

function loadChart(code, fieldKey, fieldLabel, suffix = '') {
    chartModal.show(); 
    myChart.showLoading();
    document.getElementById('chartTitle').innerText = `加载中... - ${fieldLabel}`;

    fetch(`/api/history/${code}`).then(res => res.json()).then(data => {
        myChart.hideLoading();
        document.getElementById('chartTitle').innerText = `${data.name} - ${fieldLabel} 历史趋势`;
        
        const dates = data.history.map(h => h.date);
        const seriesData = data.history.map(h => {
            let val = h[fieldKey];
            if (typeof val === 'string') val = val.replace(/,/g, '');
            return (val !== undefined && val !== null && val !== '') ? parseFloat(val) : null;
        });

        myChart.setOption({
            tooltip: { 
                trigger: 'axis',
                formatter: function (params) {
                    let item = params[0];
                    if(!item.value && item.value !== 0) return '';
                    return `${item.axisValue}<br/>${item.marker} ${fieldLabel}: ${item.value}${suffix}`;
                }
            }, 
            legend: { data: [fieldLabel] }, 
            grid: { left: '3%', right: '4%', bottom: '3%', containLabel: true },
            xAxis: { type: 'category', data: dates },
            yAxis: { type: 'value', name: fieldLabel, axisLabel: { formatter: `{value}${suffix}` } },
            series: [ 
                { name: fieldLabel, type: 'line', data: seriesData, smooth: true } 
            ]
        }, true);
    });
}

// === 定时任务 Modal ===
var scheduleModal = new bootstrap.Modal(document.getElementById('scheduleModal'));
function toggleWeekSelect() {
    const isWeekly = document.getElementById('typeWeekly').checked;
    document.getElementById('weekSelectDiv').style.display = isWeekly ? 'block' : 'none';
}
function openScheduleModal() {
    scheduleModal.show();
    fetch('/api/schedule')
        .then(res => res.json())
        .then(data => {
            document.getElementById('schedHour').value = data.hour;
            document.getElementById('schedMinute').value = data.minute;
            if (data.type === 'weekly') {
                document.getElementById('typeWeekly').checked = true;
                document.getElementById('schedWeek').value = data.day_of_week;
            } else {
                document.getElementById('typeDaily').checked = true;
            }
            toggleWeekSelect();
        });
}
function saveSchedule() {
    const hour = parseInt(document.getElementById('schedHour').value);
    const minute = parseInt(document.getElementById('schedMinute').value);
    const type = document.getElementById('typeWeekly').checked ? 'weekly' : 'daily';
    const day_of_week = document.getElementById('schedWeek').value;
    
    if (isNaN(hour)) return;

    fetch('/api/schedule', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ hour, minute, type, day_of_week })
    }).then(res => res.json()).then(data => {
        alert(data.message);
        scheduleModal.hide();
    });
}

// === 高级筛选 Modal ===
var advFilterModal = new bootstrap.Modal(document.getElementById('advancedFilterModal'));
function openAdvancedFilterModal() {
    const select = document.getElementById('advFilterSelect');
    select.innerHTML = '';
    g_columns.forEach(col => {
        if (col.no_sort && col.key !== '昨收') return; 
        let option = document.createElement('option');
        option.value = col.key;
        option.text = col.label;
        select.appendChild(option);
    });

    const listContainer = document.getElementById('activeFiltersList');
    listContainer.innerHTML = '';
    let hasActive = false;
    for (const [key, range] of Object.entries(activeFilters)) {
        if (range.min !== null || range.max !== null) {
            renderAdvFilterRow(key, range.min, range.max);
            hasActive = true;
        }
    }
    document.getElementById('emptyTip').style.display = hasActive ? 'none' : 'block';
    fetchTemplates();
    advFilterModal.show();
}

function addNewFilterRow() {
    const key = document.getElementById('advFilterSelect').value;
    if(!key) return;
    if(document.querySelector(`.adv-filter-row[data-key="${key}"]`)) return;
    renderAdvFilterRow(key, null, null);
    document.getElementById('emptyTip').style.display = 'none';
}

function renderAdvFilterRow(key, min, max) {
    const container = document.getElementById('activeFiltersList');
    const colDef = g_columns.find(c => c.key === key);
    const rowHtml = `
        <div class="card p-2 adv-filter-row shadow-sm border" data-key="${key}">
            <div class="d-flex align-items-center gap-2">
                <div class="fw-bold text-primary" style="width: 120px;">${colDef ? colDef.label : key}</div>
                <div class="input-group input-group-sm flex-grow-1">
                    <span class="input-group-text bg-white">Min</span>
                    <input type="number" class="form-control adv-min" value="${min !== null ? min : ''}">
                    <span class="input-group-text bg-white">-</span>
                    <span class="input-group-text bg-white">Max</span>
                    <input type="number" class="form-control adv-max" value="${max !== null ? max : ''}">
                </div>
                <button class="btn btn-close ms-2" onclick="this.closest('.adv-filter-row').remove()"></button>
            </div>
        </div>`;
    container.insertAdjacentHTML('beforeend', rowHtml);
}

function applyAdvancedFilter() {
    for (const key in activeFilters) {
        activeFilters[key] = { min: null, max: null };
        updateHeaderStyle(key);
    }
    
    document.querySelectorAll('.adv-filter-row').forEach(row => {
        const key = row.getAttribute('data-key');
        const minVal = row.querySelector('.adv-min').value;
        const maxVal = row.querySelector('.adv-max').value;
        
        if (minVal !== '' || maxVal !== '') {
             if (!activeFilters[key]) activeFilters[key] = {};
             // 简单处理：高级筛选暂只支持数值，文本可扩展
             activeFilters[key].min = minVal === "" ? null : parseFloat(minVal);
             activeFilters[key].max = maxVal === "" ? null : parseFloat(maxVal);
             updateHeaderStyle(key);
        }
    });
    
    executeFiltering();
    advFilterModal.hide();
}

function clearAllFilters() {
    document.getElementById('globalSearchInput').value = '';
    for (const key in activeFilters) {
        activeFilters[key] = { min: null, max: null };
        updateHeaderStyle(key);
    }
    document.getElementById('activeFiltersList').innerHTML = '';
    executeFiltering();
    advFilterModal.hide();
}

// 模版相关
function fetchTemplates() {
    fetch('/api/templates').then(res=>res.json()).then(data => {
        g_templates = data;
        const select = document.getElementById('templateSelect');
        select.innerHTML = '<option value="">-- 请选择 --</option>';
        data.forEach(t => {
            let opt = document.createElement('option');
            opt.value = t.name;
            opt.text = t.name;
            select.appendChild(opt);
        });
    });
}
function loadSelectedTemplate() {
    const name = document.getElementById('templateSelect').value;
    const t = g_templates.find(x => x.name === name);
    if(t) {
        document.getElementById('activeFiltersList').innerHTML = '';
        for (const [key, range] of Object.entries(t.filters)) {
            renderAdvFilterRow(key, range.min, range.max);
        }
        document.getElementById('emptyTip').style.display = 'none';
    }
}
function saveCurrentTemplate() {
    const name = document.getElementById('newTemplateName').value;
    const filters = {};
    document.querySelectorAll('.adv-filter-row').forEach(row => {
        const key = row.getAttribute('data-key');
        const min = row.querySelector('.adv-min').value;
        const max = row.querySelector('.adv-max').value;
        if(min !== '' || max !== '') filters[key] = { min, max };
    });
    fetch('/api/templates', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({name, filters})
    }).then(res=>res.json()).then(d => { alert(d.message); fetchTemplates(); });
}
function deleteCurrentTemplate() {
    const name = document.getElementById('templateSelect').value;
    if(!name) return;
    if(confirm('确定删除?')) {
        fetch(`/api/templates/${name}`, {method:'DELETE'}).then(res=>res.json()).then(d=>{
            alert(d.message); fetchTemplates();
        });
    }
}

function exportToClipboard() {
    if (!g_loadedData || g_loadedData.length === 0) {
        alert("当前列表中没有数据可导出！");
        return;
    }
    const textToCopy = g_loadedData.map(stock => `${stock.code}\t${stock.name}`).join('\n');
    const textArea = document.createElement("textarea");
    textArea.value = textToCopy;
    textArea.style.position = "fixed"; 
    textArea.style.left = "-9999px";
    document.body.appendChild(textArea);
    textArea.select();
    try {
        document.execCommand('copy');
        alert(`✅ 已导出 ${g_loadedData.length} 条数据到剪贴板！`);
    } catch (err) {
        alert("❌ 复制失败");
    }
    document.body.removeChild(textArea);
}