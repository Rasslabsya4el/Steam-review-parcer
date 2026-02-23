
(() => {
  const S = {
    datasets: [],
    schema: null,
    selectedDataset: "",
    includeKw: [],
    excludeKw: [],
    gameMap: new Map(),
    reviewMap: new Map(),
    rows: [],
    sortBy: "matched_reviews",
    sortOrder: "desc",
    hasQueryResult: false,
    queryRunning: false,
  };
  const R = {};
  let uid = 1;

  const OPS = {
    number: ["between", "gte", "lte", "gt", "lt", "eq", "neq", "exists", "not_exists"],
    datetime: ["between", "gte", "lte", "gt", "lt", "eq", "neq", "exists", "not_exists"],
    boolean: ["eq", "neq", "exists", "not_exists"],
    string: ["contains", "not_contains", "eq", "neq", "in", "not_in", "exists", "not_exists"],
    array_string: ["array_any", "array_all", "array_contains", "exists", "not_exists"],
    array_number: ["array_any", "array_all", "array_contains", "exists", "not_exists"],
    array_boolean: ["array_contains", "exists", "not_exists"],
    mixed: ["exists", "not_exists", "contains", "eq", "neq", "in", "not_in", "array_any", "array_all"],
  };

  const OP_LABEL = {
    eq: "equals", neq: "not equals", contains: "contains", not_contains: "not contains",
    in: "in list", not_in: "not in list", gte: ">=", lte: "<=", gt: ">", lt: "<",
    between: "between", exists: "exists", not_exists: "not exists",
    array_contains: "array contains", array_any: "array any", array_all: "array all",
  };

  const TEXT_OPS = new Set(["contains", "not_contains", "in", "not_in", "array_contains", "array_any", "array_all", "eq", "neq"]);

  const byId = (id) => document.getElementById(id);
  const arr = (x) => (Array.isArray(x) ? x : []);
  const nf = (n) => Number.isFinite(Number(n)) ? new Intl.NumberFormat("en-US").format(Math.round(Number(n))) : "0";

  const FIELD_LABEL_OVERRIDES = {
    appid: "App ID",
    app_type: "App type",
    review_url: "Review link",
    store_url: "Store link",
    hours_on_record: "Hours played",
    hours_on_record_value: "Hours played (number)",
    timestamp_created: "Review date",
    release_date_text: "Release date",
    release_year: "Release year",
    metacritic_score: "Metacritic score",
    recommendations_total: "Recommendations",
    price_initial: "Original price",
    price_final: "Current price",
    price_discount_percent: "Discount (%)",
    price_currency: "Currency",
    input_tags: "Input tags",
    search_tags: "Search tags",
  };

  const SORT_OPTIONS = [
    { field: "matched_reviews", label: "Matched reviews" },
    { field: "total_reviews", label: "Total reviews" },
    { field: "match_rate", label: "Match rate" },
    { field: "positive_matched_reviews", label: "Positive" },
    { field: "negative_matched_reviews", label: "Negative" },
    { field: "name", label: "Name" },
  ];

  function unixToDateInput(unixTs) {
    const ts = Number(unixTs);
    if (!Number.isFinite(ts)) return "";
    const ms = ts > 1e12 ? ts : ts * 1000;
    const d = new Date(ms);
    if (Number.isNaN(d.getTime())) return "";
    return d.toISOString().slice(0, 10);
  }

  function dateInputToUnix(value) {
    const text = String(value || "").trim();
    if (!text) return null;
    const parsed = Date.parse(`${text}T00:00:00Z`);
    if (!Number.isFinite(parsed)) return null;
    return Math.floor(parsed / 1000);
  }

  function looksLikeHttpUrl(value) {
    try {
      const u = new URL(String(value || ""));
      return u.protocol === "http:" || u.protocol === "https:";
    } catch (_e) {
      return false;
    }
  }

  function humanizeField(path) {
    const parts = String(path || "").split(".");
    const pretty = parts.map((part) => {
      if (FIELD_LABEL_OVERRIDES[part]) return FIELD_LABEL_OVERRIDES[part];
      let text = part.replace(/_/g, " ");
      text = text.replace(/\bappid\b/gi, "App ID");
      text = text.replace(/\burl\b/gi, "URL");
      text = text.replace(/\bid\b/gi, "ID");
      text = text.replace(/\bapi\b/gi, "API");
      text = text.replace(/\bjsonl\b/gi, "JSONL");
      return text.replace(/\b\w/g, (m) => m.toUpperCase());
    });
    return pretty.join(" > ");
  }

  function effectiveFieldKind(meta) {
    const base = String(meta?.kind || "mixed");
    if (base === "string" && meta?.datetime_like) return "datetime";
    if (base === "string" && meta?.numeric_like) return "number";
    return base;
  }

  function fieldNumericBounds(meta) {
    if (!meta || typeof meta !== "object") return [0, 100];
    const k = effectiveFieldKind(meta);
    let min = null;
    let max = null;
    if (k === "number") {
      min = Number.isFinite(Number(meta.number_min)) ? Number(meta.number_min) : null;
      max = Number.isFinite(Number(meta.number_max)) ? Number(meta.number_max) : null;
      if ((min === null || max === null) && meta.numeric_like) {
        min = Number.isFinite(Number(meta.numeric_min)) ? Number(meta.numeric_min) : min;
        max = Number.isFinite(Number(meta.numeric_max)) ? Number(meta.numeric_max) : max;
      }
    } else if (k === "datetime") {
      min = Number.isFinite(Number(meta.datetime_min)) ? Number(meta.datetime_min) : null;
      max = Number.isFinite(Number(meta.datetime_max)) ? Number(meta.datetime_max) : null;
    }
    if (min === null || max === null) return [0, 100];
    if (min === max) return [min, max + 1];
    return [Math.min(min, max), Math.max(min, max)];
  }

  function uniq(values) {
    const seen = new Set();
    const out = [];
    for (const v of values || []) {
      const s = String(v || "").trim();
      if (!s) continue;
      const k = s.toLowerCase();
      if (seen.has(k)) continue;
      seen.add(k);
      out.push(s);
    }
    return out;
  }

  function escHtml(s) {
    return String(s ?? "").replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");
  }

  function escRe(s) {
    return String(s ?? "").replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
  }

  function markText(text, keywords, caseSensitive) {
    const safe = escHtml(text);
    const kw = uniq(keywords);
    if (!kw.length) return safe;
    const p = new RegExp(`(${kw.map(escRe).sort((a, b) => b.length - a.length).join("|")})`, caseSensitive ? "g" : "gi");
    return safe.replace(p, "<mark>$1</mark>");
  }

  async function jfetch(url, options = {}) {
    const r = await fetch(url, options);
    const d = await r.json().catch(() => ({}));
    if (!r.ok) throw new Error(d.detail || JSON.stringify(d) || `HTTP ${r.status}`);
    return d;
  }

  function status(msg) {
    if (R.exploreStatus) {
      R.exploreStatus.textContent = msg;
    }
  }

  function num(el, fb = null) {
    const t = String(el?.value ?? "").trim();
    if (!t) return fb;
    const n = Number(t);
    return Number.isFinite(n) ? n : fb;
  }

  function fieldMap(scope) {
    return scope === "review" ? S.reviewMap : S.gameMap;
  }

  function fields(scope) {
    return arr(scope === "review" ? S.schema?.review_fields : S.schema?.game_fields);
  }

  function switchTabs() {
    const apply = (tab) => {
      const crawl = tab !== "explore";
      R.tabCrawlBtn.classList.toggle("active", crawl);
      R.tabExploreBtn.classList.toggle("active", !crawl);
      R.tabCrawlPanel.classList.toggle("is-active", crawl);
      R.tabExplorePanel.classList.toggle("is-active", !crawl);
      try { localStorage.setItem("steam_ui_active_tab", crawl ? "crawl" : "explore"); } catch (_e) {}
    };
    R.tabCrawlBtn.addEventListener("click", () => apply("crawl"));
    R.tabExploreBtn.addEventListener("click", () => apply("explore"));
    let saved = "crawl";
    try { saved = localStorage.getItem("steam_ui_active_tab") || "crawl"; } catch (_e) {}
    apply(saved);
  }

  function renderChips(kind) {
    const box = kind === "include" ? R.exploreIncludeKwChips : R.exploreExcludeKwChips;
    const data = kind === "include" ? S.includeKw : S.excludeKw;
    box.innerHTML = "";
    for (const kw of data) {
      const chip = document.createElement("span");
      chip.className = `chip ${kind === "exclude" ? "chip--exclude" : "chip--include"}`;
      chip.textContent = `${kind === "exclude" ? "-" : "+"} ${kw}`;
      const b = document.createElement("button");
      b.type = "button";
      b.textContent = "x";
      b.addEventListener("click", () => {
        if (kind === "include") S.includeKw = S.includeKw.filter((x) => x.toLowerCase() !== kw.toLowerCase());
        else S.excludeKw = S.excludeKw.filter((x) => x.toLowerCase() !== kw.toLowerCase());
        renderChips(kind);
      });
      chip.appendChild(b);
      box.appendChild(chip);
    }
  }

  function bindKwInput(input, kind) {
    const flush = () => {
      const list = String(input.value || "").split(",").map((x) => x.trim()).filter(Boolean);
      if (list.length) {
        if (kind === "include") S.includeKw = uniq([...S.includeKw, ...list]);
        else S.excludeKw = uniq([...S.excludeKw, ...list]);
        renderChips(kind);
      }
      input.value = "";
    };
    input.addEventListener("keydown", (e) => {
      if (e.key === "Enter" || e.key === ",") { e.preventDefault(); flush(); }
    });
    input.addEventListener("blur", flush);
  }
  function fillFieldSelect(sel, scope, selected = "") {
    sel.innerHTML = "";
    for (const f of fields(scope)) {
      const o = document.createElement("option");
      o.value = f.field;
      const cov = Math.round(Number(f.coverage || 0) * 100);
      const kind = effectiveFieldKind(f);
      const typeLabel = kind === "datetime" ? "date/time" : kind;
      o.textContent = `${humanizeField(f.field)} [${typeLabel}, ${cov}%]`;
      sel.appendChild(o);
    }
    if (selected && fields(scope).some((x) => x.field === selected)) sel.value = selected;
  }

  function fillOpSelect(sel, kind, selected = "") {
    sel.innerHTML = "";
    const ops = OPS[kind] || OPS.mixed;
    for (const op of ops) {
      const o = document.createElement("option");
      o.value = op;
      o.textContent = OP_LABEL[op] || op;
      sel.appendChild(o);
    }
    const defaultOp = (kind === "number" || kind === "datetime") ? "between" : (kind === "boolean" ? "eq" : "contains");
    sel.value = selected && ops.includes(selected) ? selected : defaultOp;
    if (!ops.includes(sel.value)) sel.value = ops[0];
  }

  function numericStep(meta) {
    const [min, max] = fieldNumericBounds(meta);
    if (!Number.isFinite(min) || !Number.isFinite(max)) return 1;
    if (Number.isInteger(min) && Number.isInteger(max)) return 1;
    return Math.abs(max - min) <= 1 ? 0.01 : 0.1;
  }

  function renderValue(row) {
    const scope = row.dataset.scope;
    const field = row.querySelector(".filter-field").value;
    const op = row.querySelector(".filter-op").value;
    const meta = fieldMap(scope).get(field) || { kind: "mixed" };
    const kind = effectiveFieldKind(meta);
    const wrap = row.querySelector(".dynamic-filter-value");
    const cwrap = row.querySelector(".filter-case-wrap");
    cwrap.style.display = (kind === "string" || kind === "array_string") && TEXT_OPS.has(op) ? "inline-flex" : "none";

    if (op === "exists" || op === "not_exists") {
      wrap.innerHTML = '<p class="muted">No value needed.</p>';
      return;
    }

    if (kind === "number") {
      const [min, max] = fieldNumericBounds(meta);
      const step = numericStep(meta);
      if (op === "between") {
        wrap.innerHTML = `
          <div class="range-sliders">
            <input type="range" data-role="range_low" min="${min}" max="${max}" step="${step}" value="${min}">
            <input type="range" data-role="range_high" min="${min}" max="${max}" step="${step}" value="${max}">
          </div>
          <div class="range-pair">
            <input type="number" data-role="value" min="${min}" max="${max}" step="${step}" value="${min}">
            <input type="number" data-role="value_to" min="${min}" max="${max}" step="${step}" value="${max}">
          </div>`;
        const rl = wrap.querySelector('[data-role="range_low"]');
        const rh = wrap.querySelector('[data-role="range_high"]');
        const il = wrap.querySelector('[data-role="value"]');
        const ih = wrap.querySelector('[data-role="value_to"]');
        const syncR = (t) => {
          let a = Number(rl.value), b = Number(rh.value);
          if (a > b) { if (t === rl) b = a; else a = b; }
          rl.value = String(a); rh.value = String(b); il.value = String(a); ih.value = String(b);
        };
        const syncI = (t) => {
          let a = Number(il.value), b = Number(ih.value);
          if (!Number.isFinite(a)) a = min; if (!Number.isFinite(b)) b = max;
          if (a > b) { if (t === il) b = a; else a = b; }
          a = Math.max(min, Math.min(max, a)); b = Math.max(min, Math.min(max, b));
          rl.value = String(a); rh.value = String(b); il.value = String(a); ih.value = String(b);
        };
        rl.addEventListener("input", () => syncR(rl)); rh.addEventListener("input", () => syncR(rh));
        il.addEventListener("input", () => syncI(il)); ih.addEventListener("input", () => syncI(ih));
      } else {
        wrap.innerHTML = `<input type="range" data-role="range_single" min="${min}" max="${max}" step="${step}" value="${min}">
          <input type="number" data-role="value" min="${min}" max="${max}" step="${step}" value="${min}">`;
        const rs = wrap.querySelector('[data-role="range_single"]');
        const iv = wrap.querySelector('[data-role="value"]');
        rs.addEventListener("input", () => { iv.value = rs.value; });
        iv.addEventListener("input", () => {
          const n = Number(iv.value); if (!Number.isFinite(n)) return;
          const c = Math.max(min, Math.min(max, n)); iv.value = String(c); rs.value = String(c);
        });
      }
      return;
    }

    if (kind === "datetime") {
      const [minTs, maxTs] = fieldNumericBounds(meta);
      const minDate = unixToDateInput(minTs);
      const maxDate = unixToDateInput(maxTs);
      if (op === "between") {
        wrap.innerHTML = `
          <div class="range-pair">
            <input type="date" data-role="value_date" ${minDate ? `min="${minDate}"` : ""} ${maxDate ? `max="${maxDate}"` : ""} value="${minDate}">
            <input type="date" data-role="value_to_date" ${minDate ? `min="${minDate}"` : ""} ${maxDate ? `max="${maxDate}"` : ""} value="${maxDate}">
          </div>`;
      } else {
        wrap.innerHTML = `<input type="date" data-role="value_date" ${minDate ? `min="${minDate}"` : ""} ${maxDate ? `max="${maxDate}"` : ""} value="${minDate}">`;
      }
      return;
    }

    if (kind === "boolean") {
      wrap.innerHTML = '<select class="inline-select" data-role="value"><option value="true">true</option><option value="false">false</option></select>';
      return;
    }

    const enumVals = arr(meta.enum_values);
    if (enumVals.length && enumVals.length <= 120 && !["in", "not_in", "array_any", "array_all"].includes(op)) {
      const sel = document.createElement("select");
      sel.className = "inline-select";
      sel.dataset.role = "value";
      for (const v of enumVals) {
        const o = document.createElement("option"); o.value = String(v); o.textContent = String(v); sel.appendChild(o);
      }
      wrap.innerHTML = "";
      wrap.appendChild(sel);
      return;
    }

    const ph = ["in", "not_in", "array_any", "array_all"].includes(op) ? "comma-separated values" : "value";
    wrap.innerHTML = `<input type="text" data-role="value" placeholder="${ph}">`;
  }

  function addFilter(scope, preset = null) {
    if (!fields(scope).length) return;
    const box = scope === "review" ? R.exploreReviewFilters : R.exploreGameFilters;
    const row = document.createElement("div");
    row.className = "dynamic-filter-row";
    row.dataset.scope = scope;
    row.dataset.uid = String(uid++);
    row.innerHTML = `
      <div class="dynamic-filter-head">
        <select class="inline-select filter-field"></select>
        <select class="inline-select filter-op"></select>
        <button type="button" class="btn-secondary btn-compact">Remove</button>
      </div>
      <div class="dynamic-filter-value"></div>
      <label class="toggle filter-case-wrap" style="display:none;">
        <input type="checkbox" class="filter-case-sensitive"><span class="toggle-text">Case-sensitive</span>
      </label>`;
    box.appendChild(row);

    const fs = row.querySelector(".filter-field");
    const os = row.querySelector(".filter-op");
    fillFieldSelect(fs, scope, preset?.field || "");
    const refresh = () => {
      const meta = fieldMap(scope).get(fs.value) || { kind: "mixed" };
      fillOpSelect(os, effectiveFieldKind(meta), preset?.op || "");
      renderValue(row);
      if (preset?.case_sensitive) row.querySelector(".filter-case-sensitive").checked = true;
      if (preset && Object.prototype.hasOwnProperty.call(preset, "value")) {
        const v = row.querySelector('[data-role="value"]');
        if (v) v.value = Array.isArray(preset.value) ? preset.value.join(", ") : String(preset.value);
        const vd = row.querySelector('[data-role="value_date"]');
        if (vd) vd.value = unixToDateInput(preset.value);
      }
      if (preset && Object.prototype.hasOwnProperty.call(preset, "value_to")) {
        const v2 = row.querySelector('[data-role="value_to"]');
        if (v2) v2.value = String(preset.value_to);
        const vd2 = row.querySelector('[data-role="value_to_date"]');
        if (vd2) vd2.value = unixToDateInput(preset.value_to);
      }
      preset = null;
    };
    fs.addEventListener("change", refresh);
    os.addEventListener("change", () => renderValue(row));
    row.querySelector("button").addEventListener("click", () => row.remove());
    refresh();
  }
  function parseValue(kind, op, raw) {
    const t = String(raw ?? "").trim();
    if (!t) return null;
    if (kind === "number") {
      const n = Number(t);
      return Number.isFinite(n) ? n : null;
    }
    if (kind === "datetime") {
      return dateInputToUnix(t);
    }
    if (kind === "boolean") {
      const x = t.toLowerCase();
      if (["true", "1", "yes", "y", "on"].includes(x)) return true;
      if (["false", "0", "no", "n", "off"].includes(x)) return false;
      return null;
    }
    if (["in", "not_in", "array_any", "array_all"].includes(op)) {
      return t.split(",").map((x) => x.trim()).filter(Boolean);
    }
    return t;
  }

  function collectFilters(scope) {
    const box = scope === "review" ? R.exploreReviewFilters : R.exploreGameFilters;
    const out = [];
    for (const row of box.querySelectorAll(".dynamic-filter-row")) {
      const field = String(row.querySelector(".filter-field")?.value || "").trim();
      const op = String(row.querySelector(".filter-op")?.value || "").trim();
      if (!field || !op) continue;
      const meta = fieldMap(scope).get(field) || { kind: "mixed" };
      const kind = effectiveFieldKind(meta);
      const item = { field, op };
      if (row.querySelector(".filter-case-sensitive")?.checked) item.case_sensitive = true;
      if (op === "exists" || op === "not_exists") {
        out.push(item);
        continue;
      }
      if (op === "between") {
        const dateA = row.querySelector('[data-role="value_date"]')?.value ?? "";
        const dateB = row.querySelector('[data-role="value_to_date"]')?.value ?? "";
        const a = kind === "datetime" ? dateInputToUnix(dateA) : Number(row.querySelector('[data-role="value"]')?.value ?? "");
        const b = kind === "datetime" ? dateInputToUnix(dateB) : Number(row.querySelector('[data-role="value_to"]')?.value ?? "");
        if (!Number.isFinite(a) || !Number.isFinite(b)) continue;
        item.value = a; item.value_to = b; out.push(item);
        continue;
      }
      const rawValue = kind === "datetime"
        ? (row.querySelector('[data-role="value_date"]')?.value ?? "")
        : (row.querySelector('[data-role="value"]')?.value ?? "");
      const val = parseValue(kind, op, rawValue);
      if (val === null || (Array.isArray(val) && !val.length)) continue;
      item.value = val;
      out.push(item);
    }
    return out;
  }

  function queryPayload() {
    return {
      include_keywords: [...S.includeKw],
      exclude_keywords: [...S.excludeKw],
      keyword_mode: R.exploreKeywordMode.value || "any",
      case_sensitive: Boolean(R.exploreCaseSensitive.checked),
      min_matched_reviews: Math.max(1, num(R.exploreMinMatchedReviews, 1)),
      game_filters: collectFilters("game"),
      review_filters: collectFilters("review"),
      sort_by: S.sortBy,
      sort_order: S.sortOrder,
      skip: 0,
      limit: Math.max(1, Math.min(500, num(R.exploreLimit, 100) || 100)),
      sample_reviews_per_game: 3,
    };
  }

  function renderSortBar() {
    if (!R.exploreSortBar) return;
    R.exploreSortBar.innerHTML = "";
    for (const item of SORT_OPTIONS) {
      const btn = document.createElement("button");
      btn.type = "button";
      btn.className = "explore-sort-btn";
      if (S.sortBy === item.field) {
        btn.classList.add("active");
        btn.textContent = `${item.label} ${S.sortOrder === "asc" ? "↑" : "↓"}`;
      } else {
        btn.textContent = item.label;
      }
      btn.addEventListener("click", async () => {
        if (S.sortBy === item.field) {
          S.sortOrder = S.sortOrder === "asc" ? "desc" : "asc";
        } else {
          S.sortBy = item.field;
          S.sortOrder = "asc";
        }
        renderSortBar();
        if (!S.hasQueryResult) {
          status(`Sort ready: ${item.label} (${S.sortOrder}). Run query to apply.`);
          return;
        }
        try {
          await runQuery();
        } catch (err) {
          status(`Query failed: ${err.message}`);
        }
      });
      R.exploreSortBar.appendChild(btn);
    }
  }

  function renderSchema(schema) {
    if (!schema) return;
  }

  function reindexSchema() {
    S.gameMap = new Map();
    S.reviewMap = new Map();
    for (const f of arr(S.schema?.game_fields)) if (f?.field) S.gameMap.set(f.field, f);
    for (const f of arr(S.schema?.review_fields)) if (f?.field) S.reviewMap.set(f.field, f);
  }

  async function loadSchema(dataset) {
    if (!dataset) return;
    status(`Loading schema for ${dataset}...`);
    const d = await jfetch(`/api/datasets/${encodeURIComponent(dataset)}/explore/schema`);
    S.schema = d;
    S.selectedDataset = dataset;
    reindexSchema();
    renderSchema(d);
    R.exploreGameFilters.innerHTML = "";
    R.exploreReviewFilters.innerHTML = "";
    R.exploreResults.innerHTML = "";
    R.exploreSummary.textContent = "No query yet.";
    R.exploreGameMeta.innerHTML = "";
    R.exploreReviewsList.innerHTML = "";
    S.rows = [];
    S.hasQueryResult = false;
    const sampledText = d.review_rows_sampled ? ` (sampled ${nf(d.review_rows_profiled || d.review_rows_count || 0)} rows)` : "";
    const cacheText = d.cache_hit ? " [cache]" : "";
    status(`Schema ready: ${nf(d.games_count)} games, ${nf(d.review_rows_count)} review rows${sampledText}.${cacheText}`);
    renderSortBar();
  }

  async function loadDatasets() {
    status("Loading datasets...");
    const d = await jfetch("/api/datasets");
    S.datasets = arr(d.datasets);
    R.exploreDatasetSelect.innerHTML = "";
    if (!S.datasets.length) {
      R.exploreDatasetSelect.disabled = true;
      R.exploreDatasetSelect.innerHTML = '<option value="">No datasets found</option>';
      status("No datasets found.");
      return;
    }
    R.exploreDatasetSelect.disabled = false;
    for (const ds of S.datasets) {
      const o = document.createElement("option");
      o.value = ds.dataset_dir;
      o.textContent = ds.label || ds.dataset_dir;
      R.exploreDatasetSelect.appendChild(o);
    }
    const selected = S.datasets.some((x) => x.dataset_dir === S.selectedDataset) ? S.selectedDataset : S.datasets[0].dataset_dir;
    R.exploreDatasetSelect.value = selected;
    await loadSchema(selected);
  }

  function renderMeta(obj) {
    const grid = document.createElement("div");
    grid.className = "explore-meta-grid";
    const entries = Object.entries(obj || {}).sort((a, b) => a[0].localeCompare(b[0]));
    for (const [k, v] of entries) {
      if (v === null || v === undefined) continue;
      if (typeof v === "string" && !v.trim()) continue;
      if (Array.isArray(v) && !v.length) continue;
      if (!Array.isArray(v) && typeof v === "object" && !Object.keys(v).length) continue;
      const card = document.createElement("div");
      card.className = "explore-meta-item";
      const keyNode = document.createElement("p");
      keyNode.className = "explore-meta-key";
      keyNode.textContent = humanizeField(k);

      const valueNode = document.createElement("p");
      valueNode.className = "explore-meta-value";
      if (typeof v === "string" && looksLikeHttpUrl(v)) {
        const a = document.createElement("a");
        a.href = v;
        a.target = "_blank";
        a.rel = "noopener noreferrer";
        a.textContent = v;
        valueNode.appendChild(a);
      } else if (Array.isArray(v)) {
        v.forEach((item, idx) => {
          const text = String(item);
          if (looksLikeHttpUrl(text)) {
            const a = document.createElement("a");
            a.href = text;
            a.target = "_blank";
            a.rel = "noopener noreferrer";
            a.textContent = text;
            valueNode.appendChild(a);
          } else {
            valueNode.appendChild(document.createTextNode(text));
          }
          if (idx < v.length - 1) {
            valueNode.appendChild(document.createTextNode(", "));
          }
        });
      } else if (typeof v === "object") {
        valueNode.textContent = JSON.stringify(v);
      } else {
        valueNode.textContent = String(v);
      }
      card.appendChild(keyNode);
      card.appendChild(valueNode);
      grid.appendChild(card);
    }
    return grid;
  }

  async function loadReviews(appid, row) {
    R.exploreReviewsList.innerHTML = '<p class="explore-empty">Loading matched reviews...</p>';
    R.exploreGameMeta.innerHTML = "";
    const head = document.createElement("p");
    head.className = "job-main";
    head.textContent = `${row?.name || "Unknown"} (App ID: ${appid})`;
    R.exploreGameMeta.appendChild(head);
    R.exploreGameMeta.appendChild(renderMeta(row?.game || {}));

    const payload = {
      include_keywords: [...S.includeKw],
      exclude_keywords: [...S.excludeKw],
      keyword_mode: R.exploreKeywordMode.value || "any",
      case_sensitive: Boolean(R.exploreCaseSensitive.checked),
      review_filters: collectFilters("review"),
      sort_by: "timestamp_desc",
      skip: 0,
      limit: 2000,
      highlight: false,
    };

    const d = await jfetch(`/api/datasets/${encodeURIComponent(S.selectedDataset)}/explore/games/${appid}/reviews`, {
      method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(payload),
    });

    const revs = arr(d.reviews);
    R.exploreReviewsList.innerHTML = "";
    if (!revs.length) {
      R.exploreReviewsList.innerHTML = '<p class="explore-empty">No matched reviews for this game.</p>';
      return;
    }
    for (const r of revs) {
      const rec = Boolean(r.recommended);
      const dt = Number.isFinite(Number(r.timestamp_created)) ? new Date(Number(r.timestamp_created) * 1000).toISOString().slice(0, 10) : "";
      const card = document.createElement("article");
      card.className = "explore-review-item";
      card.innerHTML = `
        <div class="explore-review-head">
          <span class="explore-pill ${rec ? "pos" : "neg"}">${rec ? "Recommended" : "Not Recommended"}</span>
          ${r.language ? `<span class="muted">Language: ${escHtml(r.language)}</span>` : ""}
          ${r.hours_on_record ? `<span class="muted">Playtime: ${escHtml(r.hours_on_record)}</span>` : ""}
          ${dt ? `<span class="muted">${dt}</span>` : ""}
          ${(r.review_url && looksLikeHttpUrl(r.review_url)) ? `<a class="btn-secondary btn-compact" target="_blank" rel="noopener noreferrer" href="${escHtml(r.review_url)}">Open</a>` : ""}
        </div>
        <p class="explore-review-text">${markText(r.review_text || "", S.includeKw, Boolean(R.exploreCaseSensitive.checked))}</p>`;
      R.exploreReviewsList.appendChild(card);
    }
  }
  function renderRows(rows) {
    R.exploreResults.innerHTML = "";
    if (!rows.length) {
      R.exploreResults.innerHTML = '<p class="explore-empty">No matched games.</p>';
      R.exploreGameMeta.innerHTML = "";
      R.exploreReviewsList.innerHTML = "";
      return;
    }
    for (const row of rows) {
      const appid = Number(row.appid);
      const card = document.createElement("article");
      card.className = "explore-game-row";
      const top = arr(row.matched_keywords_top).slice(0, 10).map((x) => `${x.keyword} (${x.count})`).join(", ");
      card.innerHTML = `
        <div class="explore-game-main">
          <button type="button" class="explore-game-name">${escHtml(row.name || `App ${appid}`)}</button>
          ${row.store_url ? `<a class="btn-secondary btn-compact" target="_blank" rel="noopener noreferrer" href="${escHtml(row.store_url)}">Store</a>` : ""}
        </div>
        <p class="explore-metric-line">Matched reviews: ${nf(row.matched_reviews || 0)} | Total reviews: ${nf(row.total_reviews || 0)} | Match rate: ${(Number(row.match_rate || 0) * 100).toFixed(2)}% | Positive: ${nf(row.positive_matched_reviews || 0)} | Negative: ${nf(row.negative_matched_reviews || 0)}</p>
        <p class="explore-keywords-line">${top ? `Top keywords: ${escHtml(top)}` : "Top keywords: -"}</p>`;
      card.querySelector(".explore-game-name").addEventListener("click", async () => {
        try { await loadReviews(appid, row); }
        catch (e) { R.exploreReviewsList.innerHTML = `<p class="explore-empty">Failed to load reviews: ${escHtml(e.message)}</p>`; }
      });
      R.exploreResults.appendChild(card);
    }
  }

  async function runQuery() {
    if (!S.selectedDataset) return status("Select dataset first.");
    if (S.queryRunning) return;
    S.queryRunning = true;
    status("Running query...");
    R.exploreResults.innerHTML = '<p class="explore-empty">Searching...</p>';
    try {
      const p = queryPayload();
      const d = await jfetch(`/api/datasets/${encodeURIComponent(S.selectedDataset)}/explore/query`, {
        method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(p),
      });
      S.rows = arr(d.rows);
      S.hasQueryResult = true;
      const st = d.stats || {};
      R.exploreSummary.textContent = `${nf(d.total || 0)} matched games. Scanned games: ${nf(st.games_scanned || 0)}. After game filters: ${nf(st.games_after_game_filters || 0)}. Scanned reviews: ${nf(st.reviews_scanned || 0)}. Matched reviews: ${nf(st.matched_reviews_total || 0)}.`;
      renderRows(S.rows);
      status(`Query done: ${nf(d.total || 0)} games matched, returned ${nf(S.rows.length)}.`);
      if (S.rows.length) {
        const first = S.rows[0];
        if (Number.isFinite(Number(first.appid))) {
          try { await loadReviews(Number(first.appid), first); }
          catch (e) { R.exploreReviewsList.innerHTML = `<p class="explore-empty">Failed to load reviews: ${escHtml(e.message)}</p>`; }
        }
      }
    } finally {
      S.queryRunning = false;
      renderSortBar();
    }
  }

  function mount() {
    R.tabCrawlBtn = byId("tabCrawlBtn");
    R.tabExploreBtn = byId("tabExploreBtn");
    R.tabCrawlPanel = byId("tabCrawlPanel");
    R.tabExplorePanel = byId("tabExplorePanel");

    R.exploreForm = byId("exploreForm");
    R.exploreDatasetSelect = byId("exploreDatasetSelect");
    R.exploreRefreshDatasetsBtn = byId("exploreRefreshDatasetsBtn");
    R.exploreRefreshSchemaBtn = byId("exploreRefreshSchemaBtn");
    R.exploreIncludeKwInput = byId("exploreIncludeKwInput");
    R.exploreExcludeKwInput = byId("exploreExcludeKwInput");
    R.exploreIncludeKwChips = byId("exploreIncludeKwChips");
    R.exploreExcludeKwChips = byId("exploreExcludeKwChips");
    R.exploreKeywordMode = byId("exploreKeywordMode");
    R.exploreCaseSensitive = byId("exploreCaseSensitive");
    R.exploreMinMatchedReviews = byId("exploreMinMatchedReviews");
    R.exploreLimit = byId("exploreLimit");
    R.exploreAddGameFilterBtn = byId("exploreAddGameFilterBtn");
    R.exploreAddReviewFilterBtn = byId("exploreAddReviewFilterBtn");
    R.exploreGameFilters = byId("exploreGameFilters");
    R.exploreReviewFilters = byId("exploreReviewFilters");
    R.exploreStatus = byId("exploreStatus");
    R.exploreSummary = byId("exploreSummary");
    R.exploreResults = byId("exploreResults");
    R.exploreSortBar = byId("exploreSortBar");
    R.exploreGameMeta = byId("exploreGameMeta");
    R.exploreReviewsList = byId("exploreReviewsList");
  }

  async function init() {
    mount();
    switchTabs();

    bindKwInput(R.exploreIncludeKwInput, "include");
    bindKwInput(R.exploreExcludeKwInput, "exclude");
    renderChips("include");
    renderChips("exclude");
    renderSortBar();

    R.exploreDatasetSelect.addEventListener("change", async () => {
      try { await loadSchema(R.exploreDatasetSelect.value || ""); }
      catch (e) { status(`Schema load failed: ${e.message}`); }
    });

    R.exploreRefreshDatasetsBtn.addEventListener("click", async () => {
      try { await loadDatasets(); }
      catch (e) { status(`Dataset load failed: ${e.message}`); }
    });

    R.exploreRefreshSchemaBtn.addEventListener("click", async () => {
      try { await loadSchema(S.selectedDataset); }
      catch (e) { status(`Schema load failed: ${e.message}`); }
    });

    R.exploreAddGameFilterBtn.addEventListener("click", () => addFilter("game"));
    R.exploreAddReviewFilterBtn.addEventListener("click", () => addFilter("review"));

    R.exploreForm.addEventListener("submit", async (e) => {
      e.preventDefault();
      try { await runQuery(); }
      catch (err) { status(`Query failed: ${err.message}`); R.exploreResults.innerHTML = `<p class="explore-empty">Failed: ${escHtml(err.message)}</p>`; }
    });

    try { await loadDatasets(); }
    catch (e) { status(`Dataset load failed: ${e.message}`); }
  }

  window.addEventListener("DOMContentLoaded", init);
})();
