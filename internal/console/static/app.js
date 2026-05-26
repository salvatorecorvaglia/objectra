// ================================================================
// Objectra Console — Application Logic
// ================================================================

(function () {
    'use strict';

    // ---- State ----
    let token = localStorage.getItem('objectra_token') || '';
    let currentBucket = '';
    let currentPrefix = '';

    // ---- DOM Refs ----
    const $ = (sel) => document.querySelector(sel);
    const $$ = (sel) => document.querySelectorAll(sel);

    const loginScreen = $('#login-screen');
    const dashboardScreen = $('#dashboard-screen');
    const loginForm = $('#login-form');
    const loginError = $('#login-error');
    const loginBtn = $('#login-btn');
    const logoutBtn = $('#logout-btn');

    const bucketsView = $('#buckets-view');
    const objectsView = $('#objects-view');
    const bucketsGrid = $('#buckets-grid');
    const bucketsEmpty = $('#buckets-empty');
    const bucketCount = $('#bucket-count');

    const objectsTbody = $('#objects-tbody');
    const objectsEmpty = $('#objects-empty');
    const objectsTableContainer = $('#objects-table-container');
    const objectCount = $('#object-count');
    const breadcrumb = $('#breadcrumb');

    const createBucketBtn = $('#create-bucket-btn');
    const uploadBtn = $('#upload-btn');
    const createFolderBtn = $('#create-folder-btn');
    const fileInput = $('#file-input');

    const dropZone = $('#drop-zone');
    const uploadProgress = $('#upload-progress');
    const uploadProgressFill = $('#upload-progress-fill');
    const uploadPercent = $('#upload-percent');

    // ---- API Helpers ----

    async function api(method, path, body = null, isFormData = false) {
        const opts = {
            method,
            headers: {},
        };

        if (token) {
            opts.headers['Authorization'] = `Bearer ${token}`;
        }

        if (body && !isFormData) {
            opts.headers['Content-Type'] = 'application/json';
            opts.body = JSON.stringify(body);
        } else if (body && isFormData) {
            opts.body = body;
        }

        const resp = await fetch(path, opts);
        if (resp.status === 401) {
            logout();
            throw new Error('Session expired');
        }
        return resp;
    }

    // ---- Toast ----

    function showToast(message, type = 'info') {
        const container = $('#toast-container');
        const toast = document.createElement('div');
        toast.className = `toast ${type}`;

        const icons = {
            success: '✓',
            error: '✕',
            info: 'ℹ',
        };

        toast.innerHTML = `<span>${icons[type] || ''}</span> ${escapeHtml(message)}`;
        container.appendChild(toast);

        setTimeout(() => {
            toast.style.animation = 'toastOut 0.3s ease forwards';
            setTimeout(() => toast.remove(), 300);
        }, 3500);
    }

    // ---- Auth ----

    loginForm.addEventListener('submit', async (e) => {
        e.preventDefault();
        const accessKey = $('#access-key').value.trim();
        const secretKey = $('#secret-key').value.trim();

        loginBtn.querySelector('.btn-text').style.display = 'none';
        loginBtn.querySelector('.btn-loader').style.display = 'inline-flex';
        loginError.style.display = 'none';

        try {
            const resp = await api('POST', '/api/login', { accessKey, secretKey });
            const data = await resp.json();

            if (resp.ok) {
                token = data.token;
                localStorage.setItem('objectra_token', token);
                showDashboard();
            } else {
                loginError.textContent = data.error || 'Invalid credentials';
                loginError.style.display = 'block';
            }
        } catch (err) {
            loginError.textContent = 'Connection failed. Is the server running?';
            loginError.style.display = 'block';
        } finally {
            loginBtn.querySelector('.btn-text').style.display = 'inline';
            loginBtn.querySelector('.btn-loader').style.display = 'none';
        }
    });

    logoutBtn.addEventListener('click', logout);

    function logout() {
        token = '';
        localStorage.removeItem('objectra_token');
        dashboardScreen.classList.remove('active');
        loginScreen.classList.add('active');
        loginForm.reset();
    }

    // ---- Dashboard ----

    function showDashboard() {
        loginScreen.classList.remove('active');
        dashboardScreen.classList.add('active');
        showBucketsView();
    }

    // ---- Buckets ----

    async function showBucketsView() {
        bucketsView.classList.add('active');
        objectsView.classList.remove('active');
        currentBucket = '';
        currentPrefix = '';
        await loadBuckets();
    }

    async function loadBuckets() {
        try {
            const resp = await api('GET', '/api/buckets');
            const buckets = await resp.json();

            bucketsGrid.innerHTML = '';
            if (buckets.length === 0) {
                bucketsEmpty.style.display = 'block';
                bucketCount.textContent = 'No buckets';
            } else {
                bucketsEmpty.style.display = 'none';
                bucketCount.textContent = `${buckets.length} bucket${buckets.length !== 1 ? 's' : ''}`;
                buckets.forEach((b) => {
                    bucketsGrid.appendChild(createBucketCard(b));
                });
            }
        } catch (err) {
            showToast('Failed to load buckets', 'error');
        }
    }

    function createBucketCard(bucket) {
        const card = document.createElement('div');
        card.className = 'bucket-card';

        const created = new Date(bucket.creationDate);
        const dateStr = created.toLocaleDateString('en-US', {
            year: 'numeric',
            month: 'short',
            day: 'numeric',
        });

        card.innerHTML = `
            <div class="bucket-card-header">
                <div class="bucket-card-icon">
                    <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M22 19a2 2 0 0 1-2 2H4a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h5l2 3h9a2 2 0 0 1 2 2z"/></svg>
                </div>
                <button class="btn-icon btn-danger bucket-delete-btn" title="Delete bucket" aria-label="Delete bucket ${escapeHtml(bucket.name)}" data-bucket="${escapeHtml(bucket.name)}">
                    <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><polyline points="3 6 5 6 21 6"/><path d="M19 6v14a2 2 0 0 1-2 2H7a2 2 0 0 1-2-2V6m3 0V4a2 2 0 0 1 2-2h4a2 2 0 0 1 2 2v2"/></svg>
                </button>
            </div>
            <div class="bucket-card-name">${escapeHtml(bucket.name)}</div>
            <div class="bucket-card-meta">
                <span>
                    <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><rect x="3" y="4" width="18" height="18" rx="2" ry="2"/><line x1="16" y1="2" x2="16" y2="6"/><line x1="8" y1="2" x2="8" y2="6"/><line x1="3" y1="10" x2="21" y2="10"/></svg>
                    ${dateStr}
                </span>
                <span>
                    <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M13 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V9z"/><polyline points="13 2 13 9 20 9"/></svg>
                    ${bucket.objectCount} object${bucket.objectCount !== 1 ? 's' : ''}
                </span>
            </div>
        `;

        // Click to browse bucket
        card.addEventListener('click', (e) => {
            if (e.target.closest('.bucket-delete-btn')) return;
            openBucket(bucket.name);
        });

        // Delete button
        const deleteBtn = card.querySelector('.bucket-delete-btn');
        deleteBtn.addEventListener('click', async (e) => {
            e.stopPropagation();
            if (!confirm(`Delete bucket "${bucket.name}"? It must be empty.`)) return;
            try {
                const resp = await api('DELETE', `/api/buckets/${bucket.name}`);
                if (resp.ok) {
                    showToast(`Bucket "${bucket.name}" deleted`, 'success');
                    loadBuckets();
                } else {
                    const data = await resp.json();
                    showToast(data.error || 'Failed to delete', 'error');
                }
            } catch (err) {
                showToast('Failed to delete bucket', 'error');
            }
        });

        return card;
    }

    // ---- Create Bucket Modal ----

    createBucketBtn.addEventListener('click', () => {
        openModal('create-bucket-modal');
        $('#new-bucket-name').value = '';
        $('#new-bucket-name').focus();
    });

    $('#confirm-create-bucket').addEventListener('click', async () => {
        const name = $('#new-bucket-name').value.trim();
        if (!name) return;

        try {
            const resp = await api('POST', '/api/buckets', { name });
            if (resp.ok) {
                showToast(`Bucket "${name}" created`, 'success');
                closeModal('create-bucket-modal');
                loadBuckets();
            } else {
                const data = await resp.json();
                showToast(data.error || 'Failed to create bucket', 'error');
            }
        } catch (err) {
            showToast('Failed to create bucket', 'error');
        }
    });

    // ---- Objects Browser ----

    function openBucket(name) {
        currentBucket = name;
        currentPrefix = '';
        bucketsView.classList.remove('active');
        objectsView.classList.add('active');
        loadObjects();
    }

    async function loadObjects() {
        updateBreadcrumb();

        try {
            const params = new URLSearchParams({
                prefix: currentPrefix,
                delimiter: '/',
            });
            const resp = await api('GET', `/api/buckets/${currentBucket}/objects?${params}`);
            const items = await resp.json();

            objectsTbody.innerHTML = '';

            if (items.length === 0) {
                objectsEmpty.style.display = 'block';
                objectsTableContainer.style.display = 'none';
                objectCount.textContent = 'Empty';
            } else {
                objectsEmpty.style.display = 'none';
                objectsTableContainer.style.display = 'block';
                objectCount.textContent = `${items.length} item${items.length !== 1 ? 's' : ''}`;

                items.forEach((item) => {
                    objectsTbody.appendChild(createObjectRow(item));
                });
            }
        } catch (err) {
            showToast('Failed to load objects', 'error');
        }
    }

    function createObjectRow(item) {
        const tr = document.createElement('tr');

        if (item.isPrefix) {
            // Folder
            const displayName = item.key.replace(currentPrefix, '').replace(/\/$/, '');
            tr.innerHTML = `
                <td>
                    <div class="file-name">
                        <div class="file-icon folder">
                            <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M22 19a2 2 0 0 1-2 2H4a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h5l2 3h9a2 2 0 0 1 2 2z"/></svg>
                        </div>
                        <span class="file-name-text folder-link" data-prefix="${escapeHtml(item.key)}">${escapeHtml(displayName)}/</span>
                    </div>
                </td>
                <td>—</td>
                <td>—</td>
                <td></td>
            `;

            tr.querySelector('.folder-link').addEventListener('click', () => {
                currentPrefix = item.key;
                loadObjects();
            });
        } else {
            // File
            const displayName = item.key.replace(currentPrefix, '');
            const size = formatSize(item.size);
            const modified = item.lastModified
                ? new Date(item.lastModified).toLocaleString('en-US', {
                      year: 'numeric',
                      month: 'short',
                      day: 'numeric',
                      hour: '2-digit',
                      minute: '2-digit',
                  })
                : '—';

            tr.innerHTML = `
                <td>
                    <div class="file-name">
                        <div class="file-icon file">
                            <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M13 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V9z"/><polyline points="13 2 13 9 20 9"/></svg>
                        </div>
                        <span class="file-name-text">${escapeHtml(displayName)}</span>
                    </div>
                </td>
                <td>${size}</td>
                <td>${modified}</td>
                <td>
                    <div class="file-actions">
                        <button class="btn-icon download-btn" title="Download" aria-label="Download ${escapeHtml(displayName)}" data-key="${escapeHtml(item.key)}">
                            <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"/><polyline points="7 10 12 15 17 10"/><line x1="12" y1="15" x2="12" y2="3"/></svg>
                        </button>
                        <button class="btn-icon btn-danger delete-obj-btn" title="Delete" aria-label="Delete ${escapeHtml(displayName)}" data-key="${escapeHtml(item.key)}">
                            <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><polyline points="3 6 5 6 21 6"/><path d="M19 6v14a2 2 0 0 1-2 2H7a2 2 0 0 1-2-2V6m3 0V4a2 2 0 0 1 2-2h4a2 2 0 0 1 2 2v2"/></svg>
                        </button>
                    </div>
                </td>
            `;

            tr.querySelector('.download-btn').addEventListener('click', () => {
                downloadObject(item.key);
            });

            tr.querySelector('.delete-obj-btn').addEventListener('click', async () => {
                if (!confirm(`Delete "${displayName}"?`)) return;
                try {
                    const resp = await api('DELETE', `/api/buckets/${currentBucket}/objects?key=${encodeURIComponent(item.key)}`);
                    if (resp.ok) {
                        showToast(`"${displayName}" deleted`, 'success');
                        loadObjects();
                    } else {
                        showToast('Failed to delete object', 'error');
                    }
                } catch (err) {
                    showToast('Failed to delete object', 'error');
                }
            });
        }

        return tr;
    }

    function updateBreadcrumb() {
        breadcrumb.innerHTML = '';

        // Root: Buckets
        const rootBtn = document.createElement('button');
        rootBtn.className = 'breadcrumb-item';
        rootBtn.textContent = 'Buckets';
        rootBtn.addEventListener('click', () => showBucketsView());
        breadcrumb.appendChild(rootBtn);

        addBreadcrumbSep();

        // Bucket name
        if (!currentPrefix) {
            const bucketSpan = document.createElement('span');
            bucketSpan.className = 'breadcrumb-item current';
            bucketSpan.textContent = currentBucket;
            breadcrumb.appendChild(bucketSpan);
        } else {
            const bucketBtn = document.createElement('button');
            bucketBtn.className = 'breadcrumb-item';
            bucketBtn.textContent = currentBucket;
            bucketBtn.addEventListener('click', () => {
                currentPrefix = '';
                loadObjects();
            });
            breadcrumb.appendChild(bucketBtn);

            // Prefix parts
            const parts = currentPrefix.split('/').filter(Boolean);
            parts.forEach((part, i) => {
                addBreadcrumbSep();
                const prefix = parts.slice(0, i + 1).join('/') + '/';
                if (i === parts.length - 1) {
                    const span = document.createElement('span');
                    span.className = 'breadcrumb-item current';
                    span.textContent = part;
                    breadcrumb.appendChild(span);
                } else {
                    const btn = document.createElement('button');
                    btn.className = 'breadcrumb-item';
                    btn.textContent = part;
                    btn.addEventListener('click', () => {
                        currentPrefix = prefix;
                        loadObjects();
                    });
                    breadcrumb.appendChild(btn);
                }
            });
        }
    }

    function addBreadcrumbSep() {
        const sep = document.createElement('span');
        sep.className = 'breadcrumb-sep';
        sep.textContent = '/';
        breadcrumb.appendChild(sep);
    }

    // ---- File Upload ----

    uploadBtn.addEventListener('click', () => fileInput.click());

    fileInput.addEventListener('change', (e) => {
        if (e.target.files.length > 0) {
            uploadFiles(Array.from(e.target.files));
            fileInput.value = '';
        }
    });

    // Drag and drop
    const objectsViewEl = objectsView;
    objectsViewEl.addEventListener('dragover', (e) => {
        e.preventDefault();
        dropZone.style.display = 'block';
    });

    objectsViewEl.addEventListener('dragleave', (e) => {
        if (!objectsViewEl.contains(e.relatedTarget)) {
            dropZone.style.display = 'none';
        }
    });

    objectsViewEl.addEventListener('drop', (e) => {
        e.preventDefault();
        dropZone.style.display = 'none';
        if (e.dataTransfer.files.length > 0) {
            uploadFiles(Array.from(e.dataTransfer.files));
        }
    });

    async function uploadFiles(files) {
        for (let i = 0; i < files.length; i++) {
            const file = files[i];
            const key = currentPrefix + file.name;

            uploadProgress.style.display = 'block';
            uploadPercent.textContent = `${i + 1}/${files.length}`;

            const formData = new FormData();
            formData.append('file', file);
            formData.append('key', key);

            try {
                // Use XMLHttpRequest for progress tracking
                await new Promise((resolve, reject) => {
                    const xhr = new XMLHttpRequest();
                    xhr.open('POST', `/api/buckets/${currentBucket}/objects/upload`);
                    xhr.setRequestHeader('Authorization', `Bearer ${token}`);

                    xhr.upload.onprogress = (e) => {
                        if (e.lengthComputable) {
                            const pct = Math.round((e.loaded / e.total) * 100);
                            uploadProgressFill.style.width = pct + '%';
                            uploadPercent.textContent = `${pct}% (${i + 1}/${files.length})`;
                        }
                    };

                    xhr.onload = () => {
                        if (xhr.status >= 200 && xhr.status < 300) {
                            resolve();
                        } else {
                            reject(new Error('Upload failed'));
                        }
                    };

                    xhr.onerror = () => reject(new Error('Upload failed'));
                    xhr.send(formData);
                });

                showToast(`"${file.name}" uploaded`, 'success');
            } catch (err) {
                showToast(`Failed to upload "${file.name}"`, 'error');
            }
        }

        uploadProgress.style.display = 'none';
        uploadProgressFill.style.width = '0%';
        loadObjects();
    }

    // ---- Download ----

    function downloadObject(key) {
        const url = `/api/buckets/${currentBucket}/objects/download?key=${encodeURIComponent(key)}`;
        const a = document.createElement('a');
        a.href = url;
        a.download = key.split('/').pop();

        // Add auth header via fetch
        fetch(url, {
            headers: { 'Authorization': `Bearer ${token}` },
        })
            .then((resp) => resp.blob())
            .then((blob) => {
                const blobUrl = URL.createObjectURL(blob);
                a.href = blobUrl;
                document.body.appendChild(a);
                a.click();
                document.body.removeChild(a);
                URL.revokeObjectURL(blobUrl);
            })
            .catch(() => showToast('Failed to download', 'error'));
    }

    // ---- Create Folder ----

    createFolderBtn.addEventListener('click', () => {
        openModal('create-folder-modal');
        $('#new-folder-name').value = '';
        $('#new-folder-name').focus();
    });

    $('#confirm-create-folder').addEventListener('click', async () => {
        const name = $('#new-folder-name').value.trim();
        if (!name) return;

        const key = currentPrefix + name + '/';

        // Create a "folder" by uploading a zero-byte object with a trailing slash
        const formData = new FormData();
        formData.append('file', new Blob(['']), '.keep');
        formData.append('key', key + '.objectra_folder');

        try {
            const resp = await api('POST', `/api/buckets/${currentBucket}/objects/upload`, formData, true);
            if (resp.ok) {
                showToast(`Folder "${name}" created`, 'success');
                closeModal('create-folder-modal');
                loadObjects();
            } else {
                showToast('Failed to create folder', 'error');
            }
        } catch (err) {
            showToast('Failed to create folder', 'error');
        }
    });

    // ---- Modal Helpers ----

    function openModal(id) {
        document.getElementById(id).style.display = 'flex';
    }

    function closeModal(id) {
        document.getElementById(id).style.display = 'none';
    }

    // Close modals via close buttons and overlay click
    $$('[data-close]').forEach((btn) => {
        btn.addEventListener('click', () => closeModal(btn.dataset.close));
    });

    $$('.modal-overlay').forEach((overlay) => {
        overlay.addEventListener('click', (e) => {
            if (e.target === overlay) {
                overlay.style.display = 'none';
            }
        });
    });

    // Enter key in modal inputs
    $('#new-bucket-name').addEventListener('keypress', (e) => {
        if (e.key === 'Enter') {
            e.preventDefault();
            $('#confirm-create-bucket').click();
        }
    });

    $('#new-folder-name').addEventListener('keypress', (e) => {
        if (e.key === 'Enter') {
            e.preventDefault();
            $('#confirm-create-folder').click();
        }
    });

    // ---- Utilities ----

    function formatSize(bytes) {
        if (bytes === 0) return '0 B';
        const units = ['B', 'KB', 'MB', 'GB', 'TB'];
        const i = Math.floor(Math.log(bytes) / Math.log(1024));
        const size = (bytes / Math.pow(1024, i)).toFixed(i > 0 ? 1 : 0);
        return `${size} ${units[i]}`;
    }

    function escapeHtml(str) {
        const div = document.createElement('div');
        div.textContent = str;
        return div.innerHTML;
    }

    // ---- Init ----

    function init() {
        if (token) {
            // Verify token by trying to load buckets
            api('GET', '/api/buckets')
                .then((resp) => {
                    if (resp.ok) {
                        showDashboard();
                    } else {
                        logout();
                    }
                })
                .catch(() => logout());
        }
    }

    init();
})();
