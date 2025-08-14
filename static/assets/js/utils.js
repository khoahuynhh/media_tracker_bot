// Debounce function for search input
function debounce(func, wait) {
    let timeout;
    return function executedFunction(...args) {
        const later = () => {
            clearTimeout(timeout);
            func(...args);
        };
        clearTimeout(timeout);
        timeout = setTimeout(later, wait);
    };
}

function generateSessionId() {
    return 'sess-' + Date.now() + '-' + Math.floor(Math.random() * 100000);
}

function showAlert(type, title, message) {
    const alert = document.createElement('div');
    alert.className = `alert alert-${type} alert-dismissible fade show`;
    alert.innerHTML = `
                <strong>${title}</strong> ${message}
                <button type="button" class="btn-close" data-bs-dismiss="alert"></button>
            `;
    document.querySelector('.container').insertBefore(alert, document.querySelector('.container').firstChild);

    // Auto remove after 5 seconds
    setTimeout(() => {
        if (alert.parentNode) {
            alert.remove();
        }
    }, 5000);
}

function formatDate(dateString) {
    const date = new Date(dateString);
    return date.toLocaleDateString('vi-VN');
}

// ====== SPINNER HELPERS ======
function showOverlay(el) {
  if (!el) return null;
  el.classList.add('position-relative');
  const ov = document.createElement('div');
  ov.className = 'loading-overlay';
  ov.setAttribute('aria-busy', 'true');
  ov.style.position = 'absolute';
  ov.style.inset = '0';
  ov.style.background = 'rgba(255,255,255,.6)';
  ov.style.display = 'flex';
  ov.style.alignItems = 'center';
  ov.style.justifyContent = 'center';
  ov.style.zIndex = '1056';
  ov.innerHTML = '<div class="spinner-border" role="status" aria-label="Loading"></div>';
  el.appendChild(ov);
  return ov;
}
function hideOverlay(ov) { if (ov && ov.remove) ov.remove(); }

function setBtnLoading(btn, isLoading, textWhenLoading = 'Đang xử lý…') {
  if (!btn) return;
  if (isLoading) {
    if (!btn.dataset._html) btn.dataset._html = btn.innerHTML;
    btn.disabled = true;
    btn.innerHTML = '<span class="spinner-border spinner-border-sm me-2" role="status" aria-hidden="true"></span>' + textWhenLoading;
  } else {
    btn.disabled = false;
    if (btn.dataset._html) { btn.innerHTML = btn.dataset._html; delete btn.dataset._html; }
  }
}


