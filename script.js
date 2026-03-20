const fill = document.getElementById('progress-fill');
const pct  = document.getElementById('progress-pct');
function updateProgress() {
    const scrollTop = window.scrollY;
    const docHeight = document.body.scrollHeight - window.innerHeight;
    const scrolled  = docHeight > 0 ? (scrollTop / docHeight) * 100 : 0;
    fill.style.height = scrolled + '%';
    pct.textContent   = Math.round(scrolled) + '%';
}
window.addEventListener('scroll', updateProgress);
updateProgress();
