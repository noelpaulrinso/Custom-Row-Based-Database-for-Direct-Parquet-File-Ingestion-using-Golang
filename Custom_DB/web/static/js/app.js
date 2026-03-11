// ── Global state ─────────────────────────────────────────────────────────────
let isNaturalMode = false;
let currentTables  = [];
let msgCounter     = 0;

// Conversation state
let activeConvId   = null;   // ID of the currently open conversation
let convPanelOpen  = true;   // whether the left conversations panel is visible
let renamingConvId = null;   // ID being renamed

// ── Init ──────────────────────────────────────────────────────────────────────
document.addEventListener('DOMContentLoaded', () => {
    refreshTables();
    loadImageDirectory();
    loadConversationList();

    // Auto-resize textarea
    document.getElementById('chat-input').addEventListener('input', autoResizeTextarea);
});

// ── Conversation panel toggle ─────────────────────────────────────────────────
function toggleConvPanel() {
    convPanelOpen = !convPanelOpen;
    document.getElementById('conv-panel').classList.toggle('collapsed', !convPanelOpen);
}

// ── Load & render the conversation list ──────────────────────────────────────
async function loadConversationList() {
    try {
        const res  = await fetch('/api/conversations');
        const data = await res.json();
        renderConvList(data.conversations || []);
    } catch (e) {
        console.error('Failed to load conversations', e);
    }
}

function renderConvList(convs) {
    const list = document.getElementById('conv-list');
    if (!convs || convs.length === 0) {
        list.innerHTML = '<div class="conv-list-empty">No conversations yet</div>';
        return;
    }
    list.innerHTML = convs.map(c => `
        <div class="conv-item ${c.id === activeConvId ? 'active' : ''}" id="conv-item-${c.id}" onclick="openConversation('${c.id}')">
            <div class="conv-item-title">${escapeHtml(c.title)}</div>
            <div class="conv-item-meta">${formatDate(c.updatedAt)}</div>
            <div class="conv-item-actions">
                <button class="conv-action-btn" title="Rename" onclick="startRename(event,'${c.id}','${escapeHtml(c.title)}')">✎</button>
                <button class="conv-action-btn danger" title="Delete" onclick="deleteConversation(event,'${c.id}')">✕</button>
            </div>
        </div>
    `).join('');
}

function formatDate(iso) {
    if (!iso) return '';
    try {
        const d = new Date(iso);
        const now = new Date();
        if (d.toDateString() === now.toDateString()) {
            return d.toLocaleTimeString('en-US', { hour: '2-digit', minute: '2-digit' });
        }
        return d.toLocaleDateString('en-US', { month: 'short', day: 'numeric' });
    } catch { return ''; }
}

// ── New conversation ──────────────────────────────────────────────────────────
async function newConversation() {
    try {
        const res  = await fetch('/api/conversations', { method: 'POST' });
        const data = await res.json();
        if (!data.success) throw new Error(data.error);
        await loadConversationList();
        openConversation(data.conversation.id);
    } catch (e) {
        showToast('Failed to create conversation: ' + e.message, 'error');
    }
}

// ── Open a conversation ───────────────────────────────────────────────────────
async function openConversation(id) {
    activeConvId = id;
    // Highlight in sidebar
    document.querySelectorAll('.conv-item').forEach(el => el.classList.remove('active'));
    const item = document.getElementById('conv-item-' + id);
    if (item) item.classList.add('active');

    try {
        const res  = await fetch(`/api/conversations/${id}`);
        const data = await res.json();
        if (!data.success) throw new Error(data.error);
        renderConversation(data.conversation);
    } catch (e) {
        showToast('Failed to load conversation', 'error');
    }
}

function renderConversation(conv) {
    const container = document.getElementById('chat-container');

    // Update header title
    document.getElementById('header-conv-title').textContent = conv.title || 'CustomDB';

    if (!conv.messages || conv.messages.length === 0) {
        container.innerHTML = `
            <div class="welcome-message">
                <div class="welcome-icon">⬡</div>
                <h2>CustomDB Interface</h2>
                <p>Run SQL queries or describe what you need in plain English.</p>
                <div class="example-chips">
                    <button class="chip" onclick="sendMessage('SHOW TABLES;')">SHOW TABLES</button>
                    <button class="chip" onclick="sendMessage('SELECT * FROM banking;')">SELECT * FROM banking</button>
                </div>
            </div>`;
        return;
    }

    container.innerHTML = '';
    // Walk pairs: user msg followed by bot msg
    let i = 0;
    while (i < conv.messages.length) {
        const msg = conv.messages[i];
        if (msg.role === 'user') {
            renderUserMessage(msg.text, msg.timestamp);
            i++;
        } else if (msg.role === 'bot') {
            renderBotMessage({
                success:      !msg.error,
                result:       msg.result,
                error:        msg.error,
                generatedSQL: msg.sql,
            }, msg.timestamp);
            i++;
        } else {
            i++;
        }
    }
    scrollToBottom();
}

// ── Delete conversation ───────────────────────────────────────────────────────
async function deleteConversation(evt, id) {
    evt.stopPropagation();
    if (!confirm('Delete this conversation?')) return;
    try {
        await fetch(`/api/conversations/${id}`, { method: 'DELETE' });
        if (activeConvId === id) {
            activeConvId = null;
            document.getElementById('chat-container').innerHTML = `
                <div class="welcome-message">
                    <div class="welcome-icon">⬡</div>
                    <h2>CustomDB Interface</h2>
                    <p>Select a conversation or start a new one.</p>
                </div>`;
            document.getElementById('header-conv-title').textContent = 'CustomDB';
        }
        await loadConversationList();
    } catch (e) {
        showToast('Failed to delete conversation', 'error');
    }
}

// ── Rename conversation ────────────────────────────────────────────────────────
function startRename(evt, id, currentTitle) {
    evt.stopPropagation();
    renamingConvId = id;
    document.getElementById('rename-input').value = currentTitle;
    document.getElementById('rename-dialog').classList.add('open');
    document.getElementById('rename-input').focus();
}
function closeRenameDialog() {
    document.getElementById('rename-dialog').classList.remove('open');
    renamingConvId = null;
}
async function confirmRename() {
    const title = document.getElementById('rename-input').value.trim();
    if (!title || !renamingConvId) { closeRenameDialog(); return; }
    try {
        await fetch(`/api/conversations/${renamingConvId}`, {
            method: 'PATCH',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ title }),
        });
        if (activeConvId === renamingConvId) {
            document.getElementById('header-conv-title').textContent = title;
        }
        closeRenameDialog();
        await loadConversationList();
    } catch (e) {
        showToast('Failed to rename', 'error');
    }
}

// ── Persist a message pair to the active conversation ─────────────────────────
async function persistMessage(userText, botData) {
    if (!activeConvId) return;
    try {
        await fetch(`/api/conversations/${activeConvId}/message`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ userText, botData }),
        });
        // Refresh list to update title + timestamp
        await loadConversationList();
        // Re-highlight active item
        document.querySelectorAll('.conv-item').forEach(el => el.classList.remove('active'));
        const item = document.getElementById('conv-item-' + activeConvId);
        if (item) {
            item.classList.add('active');
            // Update title display if it changed from "New Chat"
            const titleEl = item.querySelector('.conv-item-title');
            if (titleEl && titleEl.textContent === 'New Chat' && userText) {
                const words = userText.split(' ').slice(0, 8).join(' ');
                titleEl.textContent = words.length > 60 ? words.slice(0, 57) + '…' : words;
                document.getElementById('header-conv-title').textContent = titleEl.textContent;
            }
        }
    } catch (e) {
        console.warn('Failed to persist message', e);
    }
}

// ── Send a message ────────────────────────────────────────────────────────────
async function sendChatMessage() {
    const chatInput = document.getElementById('chat-input');
    const message   = chatInput.value.trim();
    if (!message) { showToast('Please enter a message', 'error'); return; }

    // Auto-create a conversation if none is active
    if (!activeConvId) {
        await newConversation();
    }

    chatInput.value = '';
    chatInput.style.height = 'auto';

    addMessage(message, 'user');
    const loadingId = addMessage('Thinking…', 'bot', true);

    try {
        const res  = await fetch('/api/query', {
            method:  'POST',
            headers: { 'Content-Type': 'application/json' },
            body:    JSON.stringify({ query: message, isNatural: isNaturalMode, conversationId: activeConvId }),
        });
        const data = await res.json();
        removeMessage(loadingId);
        displayBotResponse(data);
        persistMessage(message, data);
        if (data.success) refreshTables();
    } catch (error) {
        removeMessage(loadingId);
        addMessage('❌ Error: Failed to execute query. ' + error.message, 'bot');
        showToast('Failed to execute query', 'error');
    }
}

function sendMessage(msg) {
    document.getElementById('chat-input').value = msg;
    sendChatMessage();
}

// ── Message rendering ─────────────────────────────────────────────────────────
function addMessage(text, sender, isLoading = false) {
    const chatContainer = document.getElementById('chat-container');
    const welcomeMsg    = chatContainer.querySelector('.welcome-message');
    if (welcomeMsg) welcomeMsg.remove();

    const messageId  = 'msg-' + (++msgCounter);
    const messageDiv = document.createElement('div');
    messageDiv.className = `message ${sender}`;
    messageDiv.id        = messageId;

    const contentDiv = document.createElement('div');
    contentDiv.className = 'message-content';
    const time = getCurrentTime();

    if (sender === 'user') {
        contentDiv.innerHTML = `<div class="message-text">${escapeHtml(text)}</div><div class="message-time">${time}</div>`;
    } else {
        const header = isLoading ? '⏳ Processing' : 'CustomDB';
        contentDiv.innerHTML = `
            <div class="message-header">${header}</div>
            <div class="message-text">${escapeHtml(text)}</div>
            <div class="message-time">${time}</div>`;
    }

    messageDiv.appendChild(contentDiv);
    chatContainer.appendChild(messageDiv);
    scrollToBottom();
    return messageId;
}

function renderUserMessage(text, time) {
    const chatContainer = document.getElementById('chat-container');
    const div = document.createElement('div');
    div.className = 'message user';
    const c = document.createElement('div');
    c.className = 'message-content';
    c.innerHTML = `<div class="message-text">${escapeHtml(text)}</div><div class="message-time">${formatDate(time) || time || ''}</div>`;
    div.appendChild(c);
    chatContainer.appendChild(div);
}

function removeMessage(id) {
    const el = document.getElementById(id);
    if (el) el.remove();
}

function displayBotResponse(data) {
    renderBotMessage(data, getCurrentTime());
}

function renderBotMessage(data, time) {
    const chatContainer = document.getElementById('chat-container');
    const div = document.createElement('div');
    div.className = 'message bot';
    const c = document.createElement('div');
    c.className = 'message-content';

    let content = '<div class="message-header">CustomDB</div>';

    if (data.generatedSQL) {
        content += `<div class="message-sql">Generated SQL:<br>${escapeHtml(data.generatedSQL)}</div>`;
    }

    if (!data.success) {
        content += `<div class="result-card" style="background:#fef2f2;border:1px solid #fecaca;color:#dc2626;"><span>Error: ${escapeHtml(data.error)}</span></div>`;
    } else if (data.result) {
        const parsed = parseResultText(data.result);
        if (parsed.isTable && parsed.rows.length > 0) {
            c.className = 'message-content has-table';
            content += createTableHTML(parsed);
        } else {
            const text      = data.result.trim();
            const isModify  = /inserted|updated|deleted|affected/i.test(text);
            content += `<div class="result-card ${isModify ? 'success' : 'info'}"><span>${escapeHtml(text)}</span></div>`;
        }
    } else {
        content += `<div class="result-card success"><span>Query executed successfully.</span></div>`;
    }

    content += `<div class="message-time">${formatDate(time) || time || ''}</div>`;
    c.innerHTML = content;
    div.appendChild(c);
    chatContainer.appendChild(div);
    scrollToBottom();
}

// ── Table parsing & rendering ─────────────────────────────────────────────────
function parseResultText(text) {
    const lines = text.split('\n');
    let sepIdx  = -1;
    for (let i = 0; i < lines.length; i++) {
        if (/^-{20,}$/.test(lines[i].trimEnd())) { sepIdx = i; break; }
    }
    if (sepIdx >= 1) {
        const headerLine = lines[sepIdx - 1];
        const colWidth   = 20;
        const numCols    = Math.ceil(headerLine.trimEnd().length / colWidth) || Math.floor(lines[sepIdx].length / colWidth);
        const extractCells = (line) => {
            const cells = [];
            for (let i = 0; i < numCols; i++) {
                cells.push((line.substring(i * colWidth, (i + 1) * colWidth) || '').trim());
            }
            return cells;
        };
        const headers = extractCells(headerLine).filter(h => h.length > 0);
        if (headers.length === 0) return { isTable: false };
        const rows = [];
        for (let i = sepIdx + 1; i < lines.length; i++) {
            const line = lines[i];
            if (line.trim() === '') continue;
            const cells = extractCells(line);
            if (cells.some(c => c.length > 0)) rows.push(cells.slice(0, headers.length));
        }
        return { isTable: true, headers, rows };
    }
    if (text.includes('|')) {
        const tableLines = lines.filter(l => l.includes('|'));
        if (tableLines.length < 1) return { isTable: false };
        const headers = tableLines[0].split('|').map(h => h.trim()).filter(h => h.length > 0);
        const rows    = [];
        for (let i = 1; i < tableLines.length; i++) {
            const line = tableLines[i];
            if (/^[\s|\-]+$/.test(line)) continue;
            const cells = line.split('|').map(c => c.trim()).slice(0, headers.length);
            if (cells.some(c => c.length > 0)) rows.push(cells);
        }
        return { isTable: true, headers, rows };
    }
    return { isTable: false };
}

function createTableHTML(parsed) {
    const rowCount = parsed.rows.length;
    const colCount = parsed.headers.length;
    let html = '<div class="message-table">';
    html += `<div class="table-meta">
        <span class="table-badge cols">${colCount} col${colCount !== 1 ? 's' : ''}</span>
        <span class="table-badge rows">${rowCount} row${rowCount !== 1 ? 's' : ''}</span>
    </div>`;
    html += '<div class="table-scroll"><table class="results-table"><thead><tr>';
    for (const header of parsed.headers) html += `<th>${escapeHtml(header)}</th>`;
    html += '</tr></thead><tbody>';
    for (const row of parsed.rows) {
        html += '<tr>';
        for (let i = 0; i < parsed.headers.length; i++) {
            const val = row[i];
            if (val === undefined || val === '' || val === 'NULL' || val === 'null') {
                html += `<td class="null-val">NULL</td>`;
            } else {
                html += `<td title="${escapeHtml(val)}">${escapeHtml(val)}</td>`;
            }
        }
        html += '</tr>';
    }
    html += '</tbody></table></div></div>';
    return html;
}

// ── UI helpers ────────────────────────────────────────────────────────────────
function scrollToBottom() {
    const c = document.getElementById('chat-container');
    setTimeout(() => { c.scrollTop = c.scrollHeight; }, 100);
}

function getCurrentTime() {
    return new Date().toLocaleTimeString('en-US', { hour: '2-digit', minute: '2-digit' });
}

function autoResizeTextarea() {
    const ta = document.getElementById('chat-input');
    ta.style.height = 'auto';
    ta.style.height = Math.min(ta.scrollHeight, 150) + 'px';
}

function handleKeyPress(event) {
    if (event.ctrlKey && event.key === 'Enter') {
        event.preventDefault();
        sendChatMessage();
    }
}

function toggleMode() {
    isNaturalMode = document.getElementById('ai-mode').checked;
    const label   = document.getElementById('mode-label');
    const input   = document.getElementById('chat-input');
    if (isNaturalMode) {
        label.textContent       = '🤖 AI Mode (Natural Language)';
        input.placeholder       = 'Ask anything… e.g. show customers with balance > 5000';
    } else {
        label.textContent       = 'SQL Mode';
        input.placeholder       = 'Enter SQL query… e.g. SELECT * FROM banking;';
    }
}

function toggleDbInfo() {
    document.getElementById('sidebar').classList.toggle('open');
}

// ── Tables sidebar ────────────────────────────────────────────────────────────
async function refreshTables() {
    const tablesList = document.getElementById('tables-list');
    tablesList.innerHTML = '<div class="loading">Loading…</div>';
    try {
        const res  = await fetch('/api/tables');
        const data = await res.json();
        if (data.success && data.tables && data.tables.length > 0) {
            currentTables = data.tables;
            let html = '';
            for (const table of data.tables) {
                const columns = table.columns.map(col => `${col.name} (${col.type})`).join(', ');
                html += `
                    <div class="table-item" onclick="sendMessage('SELECT * FROM ${table.name};')">
                        <div class="table-name">📋 ${escapeHtml(table.name)}</div>
                        <div class="table-columns">${escapeHtml(columns)}</div>
                    </div>`;
            }
            tablesList.innerHTML = html;
        } else {
            tablesList.innerHTML = '<div class="loading">No tables found</div>';
        }
    } catch (error) {
        tablesList.innerHTML = '<div class="loading" style="color:var(--error);">Failed to load</div>';
    }
}

function buildSelectQuery() {
    if (currentTables.length === 0) { showToast('No tables available', 'error'); return; }
    document.getElementById('chat-input').value = `SELECT * FROM ${currentTables[0].name};`;
    document.getElementById('chat-input').focus();
}

// ── Image directory ────────────────────────────────────────────────────────────
async function loadImageDirectory() {
    try {
        const res  = await fetch('/api/image-dir');
        const data = await res.json();
        if (data.success && data.directory) document.getElementById('image-dir').value = data.directory;
    } catch (e) {}
}

async function setImageDir() {
    const directory = document.getElementById('image-dir').value.trim();
    if (!directory) { showToast('Please enter a directory path', 'error'); return; }
    try {
        const res  = await fetch('/api/image-dir', {
            method:  'POST',
            headers: { 'Content-Type': 'application/json' },
            body:    JSON.stringify({ directory }),
        });
        const data = await res.json();
        if (data.success) showToast('Image directory updated', 'success');
        else showToast('Failed to update: ' + data.error, 'error');
    } catch (e) {
        showToast('Failed to update directory', 'error');
    }
}

// ── Import / File Upload ──────────────────────────────────────────────────────
let selectedFile = null;

function openImportDialog() { document.getElementById('file-picker').click(); }

function onFileSelected(input) {
    if (!input.files || input.files.length === 0) return;
    selectedFile = input.files[0];
    const baseName = selectedFile.name.replace(/\.[^/.]+$/, '').replace(/[^a-zA-Z0-9_]/g, '_').toLowerCase();
    document.getElementById('table-name').value = baseName;
    const ext    = selectedFile.name.split('.').pop().toUpperCase();
    const sizeKB = (selectedFile.size / 1024).toFixed(1);
    const infoEl = document.getElementById('import-file-info');
    infoEl.innerHTML = `<span class="file-badge">${ext}</span><span class="file-name-text">${escapeHtml(selectedFile.name)}</span><span class="file-size-text">${sizeKB} KB</span>`;
    infoEl.style.display = 'flex';
    document.getElementById('import-dialog').classList.add('open');
    input.value = '';
}

function closeImportDialog() {
    document.getElementById('import-dialog').classList.remove('open');
    document.getElementById('import-file-info').style.display = 'none';
    selectedFile = null;
}

async function confirmImport() {
    if (!selectedFile) { showToast('No file selected', 'error'); return; }
    const tableName = document.getElementById('table-name').value.trim();
    if (!tableName) { showToast('Please enter a table name', 'error'); return; }
    const btn     = document.getElementById('import-confirm-btn');
    btn.textContent = 'Importing…';
    btn.disabled    = true;
    const formData  = new FormData();
    formData.append('file', selectedFile);
    formData.append('table_name', tableName);
    try {
        const resp = await fetch('/api/upload', { method: 'POST', body: formData });
        const data = await resp.json();
        closeImportDialog();
        if (data.success) { showToast(data.result, 'success'); displayBotResponse(data); refreshTables(); }
        else { showToast(data.error, 'error'); displayBotResponse(data); }
    } catch (err) {
        showToast('Upload failed: ' + err.message, 'error');
    } finally {
        btn.textContent = 'Import';
        btn.disabled    = false;
        document.getElementById('table-name').value = '';
    }
}

async function importSQL() {
    const sql = document.getElementById('import-sql').value.trim();
    if (!sql) { showToast('Please enter SQL commands', 'error'); return; }
    closeImportDialog();
    const commands = sql.split(';').map(c => c.trim()).filter(c => c.length > 0);
    for (const command of commands) {
        await new Promise(resolve => setTimeout(resolve, 400));
        sendMessage(command + ';');
    }
    document.getElementById('import-sql').value = '';
}

// Close modals when clicking outside
document.addEventListener('click', (e) => {
    const importDlg = document.getElementById('import-dialog');
    const renameDlg = document.getElementById('rename-dialog');
    if (e.target === importDlg) closeImportDialog();
    if (e.target === renameDlg) closeRenameDialog();
});

// ── Toast ─────────────────────────────────────────────────────────────────────
function showToast(message, type = 'info') {
    const toast = document.getElementById('toast');
    toast.textContent = message;
    toast.className   = 'toast show ' + type;
    setTimeout(() => { toast.className = 'toast'; }, 3000);
}

// ── Utils ─────────────────────────────────────────────────────────────────────
function escapeHtml(text) {
    if (typeof text !== 'string') text = String(text);
    const div = document.createElement('div');
    div.textContent = text;
    return div.innerHTML;
}


