// 使用 window 对象获取在 HTML 中注入的变量
const g_stockData = window.g_stockData || [];
const g_columns = window.g_columns || [];
let g_visibleStocks = [...g_stockData]; 

// === 懒加载配置 ===
const BATCH_SIZE = 500; // 每次加载 500 条
let g_renderedCount = 0; // 当前已渲染条数

document.addEventListener("DOMContentLoaded", function(){
    // 1. 初始化渲染表格内容
    renderTable(g_stockData);

    // 2. 修复表头 Tooltip
    document.querySelectorAll('thead [data-bs-toggle="tooltip"]').forEach(el => {
        new bootstrap.Tooltip(el, {html: true});
    });

    // 3. 滚动监听实现懒加载
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
    
    document.querySelectorAll('#tableBody .info-icon').forEach(el => {
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
        const colsHtml = g_columns.map(col => {
            let val = stock[col.key];
            if (val === undefined || val === null || val === '-' || val === '') {
                return `<td><span class="text-muted">-</span></td>`;
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
            
            if (col.no_chart) {
                return `<td>${displayVal}</td>`;
            } else {
                return `<td class="clickable-cell" onclick="loadChart('${stock.code}', '${col.key}', '${col.label}', '${suffix}')">${displayVal}</td>`;
            }
        }).join('');
        
        let nameContent = `<span class="text-truncate" style="max-width: 90px; display: inline-block; vertical-align: middle;">${stock.name}</span>`;
        if (stock.intro) {
            nameContent += `<span class="info-icon ms-1" style="vertical-align: middle; cursor: help;" data-bs-toggle="tooltip" data-bs-placement="right" title="${escapeHtml(stock.intro)}">?</span>`;
        }

        let codeColor = stock.is_ggt ? 'text-danger' : 'text-primary';
        let codeContent = `<span class="${codeColor}">${stock.code}</span>`;
        
        if (stock.is_ggt) {
            codeContent += `<span class="badge bg-primary ms-1 fw-normal" style="font-size: 10px; padding: 2px 4px; vertical-align: text-bottom;">通</span>`;
        }

        return `<tr>
            <td class="sticky-col col-code fw-bold ps-2">${codeContent}</td>
            <td class="sticky-col col-name fw-bold d-flex align-items-center justify-content-between">${nameContent}</td>
            <td class="text-muted small">${stock.date}</td>
            ${colsHtml}
        </tr>`;
    }).join('');
    
    tbody.insertAdjacentHTML('beforeend', rowsHtml);
    g_renderedCount += batch.length;

    tbody.querySelectorAll('.info-icon').forEach(el => {
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
    const filteredData = g_stockData.filter(stock => {
        for (const [key, range] of Object.entries(activeFilters)) {
            if (range.min === null && range.max === null) continue;
            let rawVal = stock[key];
            if (!rawVal || rawVal === '-' || rawVal === '') return false;
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
                    location.reload();
            }
            container.style.display = 'none';
            stopBtn.style.display = 'none';
            refreshBtn.disabled = false;
            recalcBtn.disabled = false;
        }
    });
}, 1500);

// --- 图表 ---
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

// === [新增] 定时任务设置逻辑 ===
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
        headers: {
            'Content-Type': 'application/json'
        },
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