import re
import difflib

def sanitize_text(text):
    """Remove emojis but preserve newlines and basic formatting."""
    text = re.sub(r'[\U00010000-\U0010FFFF]', '', text)
    text = re.sub(r'[\u2600-\u26FF\u2700-\u27BF]', '', text)
    lines = text.splitlines()
    cleaned_lines = []
    for line in lines:
        line = re.sub(r'\s+', ' ', line)
        cleaned_lines.append(line.strip())
    return "\n".join(cleaned_lines).strip()

def fuzzy_find(label, text, candidates, cutoff=0.6):
    """Return True if any candidate appears fuzzily in text."""
    text_low = text.lower()
    for cand in candidates:
        cand_low = cand.lower()
        if cand_low in text_low:
            return True
        if difflib.SequenceMatcher(None, cand_low, text_low).ratio() >= cutoff:
            return True
    return False

def parse_busqueda_message(content):
    content = sanitize_text(content)
    content = re.sub(r'\*\*(.*?)\*\*', r'\1', content)
    content = re.sub(r'[•⭐🕸️🕷️🌟✨💫🔥🌀🌙🌑⚡☄️🧿]', '', content)

    lines = content.split("\n")
    cleaned = [line.strip() for line in lines if line.strip()]
    joined = "\n".join(cleaned).lower()

    # Fuzzy find code
    codigo_labels = [
        "codigo de usuario",
        "codigo usuario",
        "codigo",
        "código",
        "user code",
        "codigo del usuario",
        "codigo:"
    ]
    codigo_line = None
    for line in cleaned:
        if fuzzy_find("codigo", line, codigo_labels):
            codigo_line = line
            break
    if not codigo_line:
        return None
    m = re.search(r'H?-?\s*(\d{1,4})', codigo_line, re.IGNORECASE)
    if not m:
        return None
    codigo = "H" + m.group(1).zfill(3)

    # Fuzzy find routes
    rutas_labels = [
        "rutas a visitar",
        "rutas visitar",
        "rutas",
        "ruta a visitar",
        "ruta"
    ]
    rutas_start_index = None
    for i, line in enumerate(cleaned):
        if fuzzy_find("rutas", line, rutas_labels):
            rutas_start_index = i
            break
    if rutas_start_index is None:
        return {"codigo": codigo, "rutas": []}
    rutas = []
    for line in cleaned[rutas_start_index+1:]:
        if fuzzy_find("codigo", line, codigo_labels):
            break
        line = re.sub(r'^[-•*–]+', '', line).strip()
        if not line:
            continue
        for part in line.split(","):
            p = part.strip()
            if p:
                rutas.append(p)
    return {"codigo": codigo, "rutas": rutas}