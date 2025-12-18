// 使用 window 对象获取在 HTML 中注入的变量
const g_stockData = window.g_stockData || [];
const g_columns = window.g_columns || [];
let g_visibleStocks = [...g_stockData]; 
let g_templates = []; 

// === 懒加载配置 ===
const BATCH_SIZE = 500; // 每次加载 500 条
let g_renderedCount = 0; // 当前已渲染条数

document.addEventListener("DOMContentLoaded", function(){
    renderTable(g_stockData);
    document.querySelectorAll('thead [data-bs-toggle="tooltip"]').forEach(el => {
        new bootstrap.Tooltip(el, {html: true});
    });
    const container = document.querySelector('.table-container');
    if (container) {
        container.addEventListener('scroll', function() {
            if (container.scrollTop + container.clientHeight >= container.scrollHeight - 100) {
                renderNextBatch();
            }
        });
    }
});

function escapeHtml(text) {
    if (!text) return "";
    return text.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;").replace(/"/g, "&quot;").replace(/'/g, "&#039;");
}

// === 主渲染入口 ===
function renderTable(data) {
    g_visibleStocks = data;
    g_renderedCount = 0;    
    
    const tbody = document.getElementById('tableBody');
    document.querySelectorAll('#tableBody [data-bs-toggle="tooltip"]').forEach(el => {
        const tooltip = bootstrap.Tooltip.getInstance(el);
        if (tooltip) tooltip.dispose();
    });

    tbody.innerHTML = ''; 

    if(data.length === 0) {
        tbody.innerHTML = '<tr><td colspan="100" class="text-center py-4 text-muted">没有找到匹配的股票</td></tr>';
        document.getElementById('visibleCount').innerText = 0;
        return;
    }

    document.getElementById('visibleCount').innerText = data.length;
    renderNextBatch();
    
    const container = document.querySelector('.table-container');
    if(container) container.scrollTop = 0;
}

// === 批量渲染函数 ===
function renderNextBatch() {
    if (g_renderedCount >= g_visibleStocks.length) return;

    const batch = g_visibleStocks.slice(g_renderedCount, g_renderedCount + BATCH_SIZE);
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

            // [修改] 渲染长牛评级徽章
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

            // === 估值与质量勋章逻辑 START ===
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
            // === 估值与质量勋章逻辑 END ===
            
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
    g_renderedCount += batch.length;

    tbody.querySelectorAll('[data-bs-toggle="tooltip"]').forEach(el => {
        if (!bootstrap.Tooltip.getInstance(el)) {
            new bootstrap.Tooltip(el, {html: true});
        }
    });
}

// --- 筛选与排序 ---
const activeFilters = {}; 

function confirmFilter(btn, colKey) {
    const popup = btn.closest('.filter-popup');
    const minVal = popup.querySelector(`#min-${CSS.escape(colKey)}`).value;
    const maxVal = popup.querySelector(`#max-${CSS.escape(colKey)}`).value;
    if (!activeFilters[colKey]) activeFilters[colKey] = {};
    activeFilters[colKey].min = minVal === "" ? null : parseFloat(minVal);
    activeFilters[colKey].max = maxVal === "" ? null : parseFloat(maxVal);
    updateHeaderStyle(colKey);
    setTimeout(executeFiltering, 10); 
    
    popup.style.display = 'none';
    setTimeout(() => {
        popup.style.display = '';
    }, 500);
}

function clearFilter(btn, colKey) {
    const popup = btn.closest('.filter-popup');
    popup.querySelector(`#min-${CSS.escape(colKey)}`).value = '';
    popup.querySelector(`#max-${CSS.escape(colKey)}`).value = '';
    if (activeFilters[colKey]) {
        activeFilters[colKey] = { min: null, max: null };
        updateHeaderStyle(colKey);
    }
    setTimeout(executeFiltering, 10);
}

function updateHeaderStyle(colKey) {
    const th = document.querySelector(`th[data-key="${colKey}"]`);
    const filter = activeFilters[colKey];
    if (filter && (filter.min !== null || filter.max !== null)) th.classList.add('filter-active');
    else th.classList.remove('filter-active');
}

function executeFiltering() {
    const searchVal = document.getElementById('globalSearchInput').value.trim().toLowerCase();

    const filteredData = g_stockData.filter(stock => {
        if (searchVal) {
            const code = String(stock.code).toLowerCase();
            const name = String(stock.name).toLowerCase();
            if (!code.includes(searchVal) && !name.includes(searchVal)) {
                return false;
            }
        }

        for (const [key, range] of Object.entries(activeFilters)) {
            if (range.min === null && range.max === null) continue;
            let rawVal = stock[key];
            if (!rawVal || rawVal === '-' || rawVal === '') return false;
            
            // 简单的文本包含过滤 (针对长牛标签)
            if (key === 'bull_label') {
                // 如果设置了min/max其实对字符串无效，这里保留逻辑兼容
                // 实际建议增加文本筛选器，或者简单跳过字符串列的数值筛选
                continue; 
            }

            let val = parseFloat(String(rawVal).replace(/,/g, '').replace('%', ''));
            if (range.min !== null && val < range.min) return false;
            if (range.max !== null && val > range.max) return false;
        }
        return true;
    });
    
    if(sortState.key) {
        g_visibleStocks = filteredData;
        doSort(sortState.key, sortState.type, false);
        renderTable(g_visibleStocks);
    } else {
        renderTable(filteredData);
    }
}

let sortState = { key: '', dir: 'asc', type: '' };

function sortTable(key, type) {
    document.querySelectorAll('.sort-icon').forEach(el => el.innerText = '');
    if (sortState.key === key) sortState.dir = sortState.dir === 'asc' ? 'desc' : 'asc';
    else { sortState.key = key; sortState.dir = 'asc'; }
    sortState.type = type; 
    
    const ths = document.querySelectorAll('thead th');
    ths.forEach(th => {
        const div = th.querySelector('.th-content');
        if(div && div.onclick && div.onclick.toString().includes(`'${key}'`)) {
            th.querySelector('.sort-icon').innerText = sortState.dir === 'asc' ? ' ▲' : ' ▼';
        }
    });
    
    doSort(key, type, true);
}

function doSort(key, type, shouldRender = true) {
    g_visibleStocks.sort((a, b) => {
        let valA = a[key];
        let valB = b[key];
        
        if (type === 'numeric') {
            valA = (valA === '-' || !valA) ? -Infinity : parseFloat(String(valA).replace(/,/g, ''));
            valB = (valB === '-' || !valB) ? -Infinity : parseFloat(String(valB).replace(/,/g, ''));
            return sortState.dir === 'asc' ? valA - valB : valB - valA;
        } else {
            valA = valA || ""; valB = valB || "";
            return sortState.dir === 'asc' ? valA.localeCompare(valB) : valB.localeCompare(valA);
        }
    });
    
    if (shouldRender) {
        renderTable(g_visibleStocks);
    }
}

// --- API 交互 ---
function triggerRecalculate() {
    if(!confirm("确定补全吗？")) return;
    document.getElementById('recalcBtn').disabled = true;
    fetch('/api/recalculate', { method: 'POST' });
}

// [修改] 触发长牛趋势分析
function triggerTrendAnalysis() {
    if(!confirm("确定要执行【5年分级】长牛筛选吗？\n\n这需要下载过去5年的K线数据，耗时较长，请耐心等待。")) return;
    
    const btn = document.getElementById('trendBtn');
    btn.disabled = true;
    
    fetch('/api/analyze_trends', { method: 'POST' })
        .then(res => res.json())
        .then(data => {
            alert(data.message);
        })
        .catch(err => {
            alert("请求失败");
            btn.disabled = false;
        });
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
        const trendBtn = document.getElementById('trendBtn'); 

        if (data.is_running) {
            container.style.display = 'block';
            stopBtn.style.display = 'inline-block';
            refreshBtn.disabled = true;
            recalcBtn.disabled = true;
            if(trendBtn) trendBtn.disabled = true; 
            
            const pct = data.total > 0 ? Math.round(data.current/data.total*100) : 0;
            document.getElementById('progress-bar').style.width = pct + "%";
            document.getElementById('progress-msg').innerText = `${data.message}`;
        } else {
            if (container.style.display === 'block') {
                    location.reload();
            }
            container.style.display = 'none';
            stopBtn.style.display = 'none';
            refreshBtn.disabled = false;
            recalcBtn.disabled = false;
            if(trendBtn) trendBtn.disabled = false; 
        }
    });
}, 1500);

var myChart = echarts.init(document.getElementById('chart-container'));
var chartModal = new bootstrap.Modal(document.getElementById('chartModal'));
document.getElementById('chartModal').addEventListener('shown.bs.modal', () => myChart.resize());

function loadChart(code, fieldKey, fieldLabel, suffix = '') {
    chartModal.show(); 
    myChart.showLoading();
    
    const stockName = g_stockData.find(s => s.code == code)?.name || code;
    const title = `${stockName} - ${fieldLabel} 历史趋势`;
    document.getElementById('chartTitle').innerText = title;

    fetch(`/api/history/${code}`).then(res => res.json()).then(data => {
        myChart.hideLoading();
        
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

// ... (以下定时任务和筛选模版代码逻辑保持不变，为节省篇幅略去，实际使用请保留原文件的这部分代码) ...
// 请确保将原文件 script.js 中从 'var scheduleModal' 到底部的代码完整保留。
// (为保证文件完整性，这里我还是贴出剩余部分)

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
        })
        .catch(err => {
            console.error(err);
            alert('获取当前配置失败');
        });
}

function saveSchedule() {
    const hour = parseInt(document.getElementById('schedHour').value);
    const minute = parseInt(document.getElementById('schedMinute').value);
    const type = document.getElementById('typeWeekly').checked ? 'weekly' : 'daily';
    const day_of_week = document.getElementById('schedWeek').value;
    
    if (isNaN(hour) || hour < 0 || hour > 23 || isNaN(minute) || minute < 0 || minute > 59) {
        alert("请输入正确的时间（0-23时，0-59分）");
        return;
    }
    
    const btn = document.querySelector('#scheduleModal .btn-primary');
    const originalText = btn.innerText;
    btn.disabled = true;
    btn.innerText = "保存中...";
    
    fetch('/api/schedule', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ 
            hour: hour, 
            minute: minute,
            type: type,
            day_of_week: day_of_week
        })
    })
    .then(res => res.json())
    .then(data => {
        if (data.success) {
            alert(data.message);
            scheduleModal.hide();
        } else {
            alert("保存失败: " + data.message);
        }
    })
    .catch(err => {
        alert("网络错误");
    })
    .finally(() => {
        btn.disabled = false;
        btn.innerText = originalText;
    });
}

var advFilterModal = new bootstrap.Modal(document.getElementById('advancedFilterModal'));

function openAdvancedFilterModal() {
    const listContainer = document.getElementById('activeFiltersList');
    const select = document.getElementById('advFilterSelect');
    
    select.innerHTML = '';
    g_columns.forEach(col => {
        if (col.no_sort && col.key !== '昨收') return; 
        let option = document.createElement('option');
        option.value = col.key;
        option.text = col.label;
        select.appendChild(option);
    });

    listContainer.innerHTML = '';
    let hasActive = false;
    for (const [key, range] of Object.entries(activeFilters)) {
        if (range.min !== null || range.max !== null) {
            renderFilterRow(key, range.min, range.max);
            hasActive = true;
        }
    }
    document.getElementById('emptyTip').style.display = hasActive ? 'none' : 'block';
    fetchTemplates();
    advFilterModal.show();
}

function fetchTemplates() {
    fetch('/api/templates')
        .then(res => res.json())
        .then(data => {
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
    if (!name) return;
    const template = g_templates.find(t => t.name === name);
    if (!template) return;
    document.getElementById('activeFiltersList').innerHTML = '';
    if (Object.keys(template.filters).length > 0) {
        document.getElementById('emptyTip').style.display = 'none';
        for (const [key, range] of Object.entries(template.filters)) {
            renderFilterRow(key, range.min, range.max);
        }
    } else {
        document.getElementById('emptyTip').style.display = 'block';
    }
}

function saveCurrentTemplate() {
    const nameInput = document.getElementById('newTemplateName');
    const name = nameInput.value.trim();
    if (!name) {
        alert("请输入模版名称");
        return;
    }
    const filters = {};
    const rows = document.querySelectorAll('.adv-filter-row');
    rows.forEach(row => {
        const key = row.getAttribute('data-key');
        const minVal = row.querySelector('.adv-min').value;
        const maxVal = row.querySelector('.adv-max').value;
        if (minVal !== '' || maxVal !== '') {
            filters[key] = {
                min: minVal === "" ? null : parseFloat(minVal),
                max: maxVal === "" ? null : parseFloat(maxVal)
            };
        }
    });
    if (Object.keys(filters).length === 0) {
        alert("请先添加至少一个筛选条件");
        return;
    }
    fetch('/api/templates', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ name: name, filters: filters })
    })
    .then(res => res.json())
    .then(data => {
        if(data.success) {
            alert("保存成功");
            nameInput.value = '';
            fetchTemplates(); 
        } else {
            alert("保存失败: " + data.message);
        }
    });
}

function deleteCurrentTemplate() {
    const name = document.getElementById('templateSelect').value;
    if (!name) {
        alert("请先选择一个要删除的模版");
        return;
    }
    if (!confirm(`确定要删除模版 "${name}" 吗？`)) return;
    fetch(`/api/templates/${encodeURIComponent(name)}`, { method: 'DELETE' })
        .then(res => res.json())
        .then(data => {
            if(data.success) {
                alert("删除成功");
                fetchTemplates();
                document.getElementById('templateSelect').value = "";
            } else {
                alert("删除失败: " + data.message);
            }
        });
}

function addNewFilterRow() {
    const select = document.getElementById('advFilterSelect');
    const key = select.value;
    if(!key) return;
    const existingRow = document.querySelector(`.adv-filter-row[data-key="${key}"]`);
    if(existingRow) {
        existingRow.classList.add('bg-warning');
        setTimeout(() => existingRow.classList.remove('bg-warning'), 500);
        existingRow.scrollIntoView({behavior: 'smooth', block: 'center'});
        return;
    }
    renderFilterRow(key, null, null);
    document.getElementById('emptyTip').style.display = 'none';
}

function renderFilterRow(key, min, max) {
    const container = document.getElementById('activeFiltersList');
    const colDef = g_columns.find(c => c.key === key);
    if (!colDef) return;
    const rowHtml = `
        <div class="card p-2 adv-filter-row shadow-sm border" data-key="${key}" style="transition: background 0.3s;">
            <div class="d-flex align-items-center gap-2">
                <div class="fw-bold text-primary" style="width: 120px;">${colDef.label}</div>
                <div class="input-group input-group-sm flex-grow-1">
                    <span class="input-group-text bg-white">Min</span>
                    <input type="number" class="form-control adv-min" placeholder="最小值" value="${min !== null ? min : ''}">
                    <span class="input-group-text bg-white">-</span>
                    <span class="input-group-text bg-white">Max</span>
                    <input type="number" class="form-control adv-max" placeholder="最大值" value="${max !== null ? max : ''}">
                </div>
                <button class="btn btn-close ms-2" onclick="removeFilterRow(this)"></button>
            </div>
        </div>
    `;
    container.insertAdjacentHTML('beforeend', rowHtml);
}

function removeFilterRow(btn) {
    const row = btn.closest('.adv-filter-row');
    row.remove();
    const container = document.getElementById('activeFiltersList');
    if(container.children.length === 0) {
        document.getElementById('emptyTip').style.display = 'block';
    }
}

function applyAdvancedFilter() {
    for (const key in activeFilters) {
        activeFilters[key] = { min: null, max: null };
        updateHeaderStyle(key);
    }
    const rows = document.querySelectorAll('.adv-filter-row');
    rows.forEach(row => {
        const key = row.getAttribute('data-key');
        const minInput = row.querySelector('.adv-min');
        const maxInput = row.querySelector('.adv-max');
        const minVal = minInput.value;
        const maxVal = maxInput.value;
        if (minVal !== '' || maxVal !== '') {
            if (!activeFilters[key]) activeFilters[key] = {};
            activeFilters[key].min = minVal === "" ? null : parseFloat(minVal);
            activeFilters[key].max = maxVal === "" ? null : parseFloat(maxVal);
            updateHeaderStyle(key);
            const headerPopup = document.querySelector(`th[data-key="${key}"] .filter-popup`);
            if (headerPopup) {
                headerPopup.querySelector(`#min-${CSS.escape(key)}`).value = minVal;
                headerPopup.querySelector(`#max-${CSS.escape(key)}`).value = maxVal;
            }
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
        const headerPopup = document.querySelector(`th[data-key="${key}"] .filter-popup`);
        if (headerPopup) {
            headerPopup.querySelector(`#min-${CSS.escape(key)}`).value = '';
            headerPopup.querySelector(`#max-${CSS.escape(key)}`).value = '';
        }
    }
    document.getElementById('activeFiltersList').innerHTML = '';
    document.getElementById('emptyTip').style.display = 'block';
    executeFiltering();
    advFilterModal.hide(); 
}

function exportToClipboard() {
    if (!g_visibleStocks || g_visibleStocks.length === 0) {
        alert("当前列表中没有数据可导出！");
        return;
    }
    const textToCopy = g_visibleStocks.map(stock => `${stock.code}\t${stock.name}`).join('\n');
    if (navigator.clipboard && window.isSecureContext) {
        navigator.clipboard.writeText(textToCopy).then(() => {
            alert(`✅ 已成功复制 ${g_visibleStocks.length} 条数据到剪贴板！\n格式：代码 + Tab + 名称`);
        }).catch(err => {
            console.error('Clipboard API failed:', err);
            fallbackCopy(textToCopy);
        });
    } else {
        fallbackCopy(textToCopy);
    }
}

function fallbackCopy(text) {
    const textArea = document.createElement("textarea");
    textArea.value = text;
    textArea.style.position = "fixed"; 
    textArea.style.left = "-9999px";
    document.body.appendChild(textArea);
    textArea.focus();
    textArea.select();
    try {
        const successful = document.execCommand('copy');
        if (successful) {
            alert(`✅ 已成功复制 ${g_visibleStocks.length} 条数据到剪贴板！\n(兼容模式)`);
        } else {
            alert("❌ 复制失败，请重试");
        }
    } catch (err) {
        console.error('Fallback copy failed:', err);
        alert("❌ 浏览器不支持自动复制，请手动操作");
    }
    document.body.removeChild(textArea);
}