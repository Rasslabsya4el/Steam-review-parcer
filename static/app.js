const state = {
  defaults: null,
  tags: [],
  appTypes: [],
  reviewSources: [],
  includeTags: new Set(),
  excludeTags: new Set(),
  includeTypes: new Set(),
  excludeTypes: new Set(),
  currentJobId: null,
  pollTimer: null,
  estimateTimer: null,
  estimateAbort: null,
  scopeEstimate: null,
};

const refs = {};

function byId(id) {
  return document.getElementById(id);
}

function parseIntOrNull(value) {
  const trimmed = String(value ?? "").trim();
  if (!trimmed) return null;
  const parsed = Number.parseInt(trimmed, 10);
  return Number.isFinite(parsed) ? parsed : null;
}

function parseFloatOrNull(value) {
  const trimmed = String(value ?? "").trim();
  if (!trimmed) return null;
  const parsed = Number.parseFloat(trimmed);
  return Number.isFinite(parsed) ? parsed : null;
}

function dedupeArray(values) {
  const unique = [];
  const seen = new Set();
  for (const value of values) {
    const clean = String(value ?? "").trim();
    if (!clean) continue;
    const key = clean.toLowerCase();
    if (seen.has(key)) continue;
    seen.add(key);
    unique.push(clean);
  }
  return unique;
}

function setBackendBadge(type, text) {
  refs.backendBadge.className = `badge ${type}`;
  refs.backendBadge.textContent = text;
}

function formatPercent(progress) {
  const value = Number.isFinite(progress) ? Math.min(1, Math.max(0, progress)) : 0;
  return `${Math.round(value * 100)}%`;
}

function formatInt(value) {
  const number = Number(value);
  if (!Number.isFinite(number)) return "0";
  return new Intl.NumberFormat("en-US").format(Math.round(number));
}

function compactListPreview(values, maxItems = 4, fallback = "-", mapFn = null) {
  const list = dedupeArray(Array.isArray(values) ? values : []);
  const mapped = mapFn ? list.map((x) => mapFn(x)) : list;
  if (mapped.length === 0) return fallback;
  if (mapped.length <= maxItems) return mapped.join(", ");
  const head = mapped.slice(0, maxItems).join(", ");
  return `${head} +${mapped.length - maxItems}`;
}

function appTypeLabel(value) {
  const key = String(value || "").trim().toLowerCase();
  const match = state.appTypes.find((x) => String(x.value || "").trim().toLowerCase() === key);
  return String(match && match.label ? match.label : value || "");
}

function tagLabel(value) {
  const key = String(value || "").trim().toLowerCase();
  const match = state.tags.find((x) => String(x.name || "").trim().toLowerCase() === key);
  return String(match && match.name ? match.name : value || "");
}

function summarizeJobForList(job) {
  const params = job && typeof job.params === "object" ? job.params : {};
  const dataset = String(params.dataset_dir || "-");
  const progress = formatPercent(Number(job.progress || 0));
  const jobType = String(job.type || "");

  if (jobType === "crawl") {
    const includeTypes = compactListPreview(params.include_app_types || [], 3, "all", appTypeLabel);
    const excludeTypes = compactListPreview(params.exclude_app_types || [], 3, "", appTypeLabel);
    const includeTags = compactListPreview(params.tags || [], 5, "-", null);
    const excludeTags = compactListPreview(params.exclude_tags || [], 4, "", null);
    const typeLine = excludeTypes ? `types: ${includeTypes} | exclude: ${excludeTypes}` : `types: ${includeTypes}`;
    const tagLine = excludeTags ? `tags: ${includeTags} | untag: ${excludeTags}` : `tags: ${includeTags}`;
    return {
      main: dataset,
      sub: `${typeLine} | ${tagLine} | ${progress}`,
    };
  }

  if (jobType === "search") {
    const keywords = compactListPreview(params.keywords || [], 5, "-");
    return {
      main: dataset,
      sub: `keywords: ${keywords} | ${progress}`,
    };
  }

  return {
    main: dataset,
    sub: progress,
  };
}

function applyLayout(mode) {
  if (mode === "top") {
    refs.mainLayout.classList.remove("layout-grid--sidebar");
    refs.mainLayout.classList.add("layout-grid--top");
    refs.layoutSidebarBtn.classList.remove("active");
    refs.layoutTopBtn.classList.add("active");
    return;
  }
  refs.mainLayout.classList.remove("layout-grid--top");
  refs.mainLayout.classList.add("layout-grid--sidebar");
  refs.layoutTopBtn.classList.remove("active");
  refs.layoutSidebarBtn.classList.add("active");
}

function setTagRule(value, mode) {
  const v = String(value || "").trim();
  if (!v) return;

  if (mode === "include") {
    state.excludeTags.delete(v);
    state.includeTags.add(v);
  } else if (mode === "exclude") {
    state.includeTags.delete(v);
    state.excludeTags.add(v);
  } else {
    state.includeTags.delete(v);
    state.excludeTags.delete(v);
  }

  renderTagRuleOptions();
  renderTagRuleChips();
  updatePayloadPreview();
  scheduleScopeEstimateRefresh();
}

function renderTagRuleChips() {
  refs.selectedTagRules.innerHTML = "";

  const includeValues = [...state.includeTags].sort((a, b) => String(a).localeCompare(String(b)));
  const excludeValues = [...state.excludeTags].sort((a, b) => String(a).localeCompare(String(b)));

  function makeChip(value, mode) {
    const chip = document.createElement("span");
    chip.className = `chip ${mode === "exclude" ? "chip--exclude" : "chip--include"}`;
    chip.textContent = `${mode === "exclude" ? "-" : "+"} ${tagLabel(value)}`;

    const removeBtn = document.createElement("button");
    removeBtn.type = "button";
    removeBtn.textContent = "x";
    removeBtn.title = `Remove ${mode} rule for ${value}`;
    removeBtn.addEventListener("click", () => setTagRule(value, "none"));

    chip.appendChild(removeBtn);
    refs.selectedTagRules.appendChild(chip);
  }

  for (const value of includeValues) {
    makeChip(value, "include");
  }
  for (const value of excludeValues) {
    makeChip(value, "exclude");
  }
}

function renderTagRuleOptions() {
  const filter = refs.tagRuleSearch.value.trim().toLowerCase();
  refs.tagRuleOptions.innerHTML = "";

  const fragment = document.createDocumentFragment();
  const sorted = [...state.tags].sort((a, b) => String(a.name || "").localeCompare(String(b.name || "")));
  for (const tag of sorted) {
    const tagName = String(tag.name || "").trim();
    const tagId = tag.tagid;
    if (!tagName) continue;

    const matchTarget = `${tagName} ${tagId}`.toLowerCase();
    if (filter && !matchTarget.includes(filter)) continue;

    const includeActive = state.includeTags.has(tagName);
    const excludeActive = state.excludeTags.has(tagName);

    const row = document.createElement("div");
    row.className = "rule-row";
    if (includeActive) row.classList.add("rule-row--include");
    if (excludeActive) row.classList.add("rule-row--exclude");

    const includeLabel = document.createElement("label");
    includeLabel.className = "rule-include";
    includeLabel.title = "Include tag";

    const includeInput = document.createElement("input");
    includeInput.type = "checkbox";
    includeInput.checked = includeActive;
    includeInput.addEventListener("change", () => {
      if (includeInput.checked) setTagRule(tagName, "include");
      else setTagRule(tagName, "none");
    });

    includeLabel.appendChild(includeInput);

    const nameWrap = document.createElement("span");
    nameWrap.className = "rule-name-wrap";

    const name = document.createElement("span");
    name.className = "rule-name";
    name.textContent = tagName;

    const idText = document.createElement("span");
    idText.className = "rule-id";
    idText.textContent = `#${tagId}`;

    nameWrap.appendChild(name);
    nameWrap.appendChild(idText);

    const excludeBtn = document.createElement("button");
    excludeBtn.type = "button";
    excludeBtn.className = "rule-exclude-btn";
    if (excludeActive) excludeBtn.classList.add("active");
    excludeBtn.textContent = "-";
    excludeBtn.title = excludeActive ? "Remove exclude rule" : "Exclude tag";
    excludeBtn.addEventListener("click", () => {
      if (excludeActive) setTagRule(tagName, "none");
      else setTagRule(tagName, "exclude");
    });

    row.appendChild(includeLabel);
    row.appendChild(nameWrap);
    row.appendChild(excludeBtn);
    fragment.appendChild(row);
  }

  refs.tagRuleOptions.appendChild(fragment);
}

function setTypeRule(value, mode) {
  const v = String(value || "").trim().toLowerCase();
  if (!v) return;

  if (mode === "include") {
    state.excludeTypes.delete(v);
    state.includeTypes.add(v);
  } else if (mode === "exclude") {
    state.includeTypes.delete(v);
    state.excludeTypes.add(v);
  } else {
    state.includeTypes.delete(v);
    state.excludeTypes.delete(v);
  }

  renderTypeRuleOptions();
  renderTypeRuleChips();
  updatePayloadPreview();
  scheduleScopeEstimateRefresh();
}

function renderTypeRuleChips() {
  refs.selectedTypeRules.innerHTML = "";

  const includeValues = [...state.includeTypes].sort((a, b) => a.localeCompare(b));
  const excludeValues = [...state.excludeTypes].sort((a, b) => a.localeCompare(b));

  function makeChip(value, mode) {
    const chip = document.createElement("span");
    chip.className = `chip ${mode === "exclude" ? "chip--exclude" : "chip--include"}`;
    chip.textContent = `${mode === "exclude" ? "-" : "+"} ${appTypeLabel(value)}`;

    const removeBtn = document.createElement("button");
    removeBtn.type = "button";
    removeBtn.textContent = "x";
    removeBtn.title = `Remove ${mode} rule for ${value}`;
    removeBtn.addEventListener("click", () => setTypeRule(value, "none"));

    chip.appendChild(removeBtn);
    refs.selectedTypeRules.appendChild(chip);
  }

  for (const value of includeValues) {
    makeChip(value, "include");
  }
  for (const value of excludeValues) {
    makeChip(value, "exclude");
  }
}

function renderTypeRuleOptions() {
  const filter = refs.typeRuleSearch.value.trim().toLowerCase();
  refs.typeRuleOptions.innerHTML = "";

  const fragment = document.createDocumentFragment();
  const sorted = [...state.appTypes].sort((a, b) => String(a.label || "").localeCompare(String(b.label || "")));
  for (const item of sorted) {
    const value = String(item.value || "").trim().toLowerCase();
    const label = String(item.label || value || "");
    if (!value) continue;

    const matchTarget = `${value} ${label}`.toLowerCase();
    if (filter && !matchTarget.includes(filter)) continue;

    const includeActive = state.includeTypes.has(value);
    const excludeActive = state.excludeTypes.has(value);

    const row = document.createElement("div");
    row.className = "rule-row";
    if (includeActive) row.classList.add("rule-row--include");
    if (excludeActive) row.classList.add("rule-row--exclude");

    const includeLabel = document.createElement("label");
    includeLabel.className = "rule-include";
    includeLabel.title = "Include app type";

    const includeInput = document.createElement("input");
    includeInput.type = "checkbox";
    includeInput.checked = includeActive;
    includeInput.addEventListener("change", () => {
      if (includeInput.checked) setTypeRule(value, "include");
      else setTypeRule(value, "none");
    });

    includeLabel.appendChild(includeInput);

    const nameWrap = document.createElement("span");
    nameWrap.className = "rule-name-wrap";

    const name = document.createElement("span");
    name.className = "rule-name";
    name.textContent = label;

    const valueText = document.createElement("span");
    valueText.className = "rule-id";
    valueText.textContent = value;

    nameWrap.appendChild(name);
    nameWrap.appendChild(valueText);

    const excludeBtn = document.createElement("button");
    excludeBtn.type = "button";
    excludeBtn.className = "rule-exclude-btn";
    if (excludeActive) excludeBtn.classList.add("active");
    excludeBtn.textContent = "-";
    excludeBtn.title = excludeActive ? "Remove exclude rule" : "Exclude app type";
    excludeBtn.addEventListener("click", () => {
      if (excludeActive) setTypeRule(value, "none");
      else setTypeRule(value, "exclude");
    });

    row.appendChild(includeLabel);
    row.appendChild(nameWrap);
    row.appendChild(excludeBtn);
    fragment.appendChild(row);
  }

  refs.typeRuleOptions.appendChild(fragment);
}

function setScopeEstimateMessage(text, mode = "") {
  refs.scopeEstimateHint.textContent = text;
  refs.scopeEstimateHint.className = "scope-estimate";
  if (mode) {
    refs.scopeEstimateHint.classList.add(mode);
  }
}

function buildEstimatePayload() {
  return {
    tags: [...state.includeTags],
    exclude_tags: [...state.excludeTags],
    include_app_types: [...state.includeTypes],
    exclude_app_types: [...state.excludeTypes],
    timeout: parseIntOrNull(refs.timeout.value) || 30,
    search_page_size: parseIntOrNull(refs.searchPageSize.value) || 50,
    sample_size: 60,
    max_games: parseIntOrNull(refs.maxGames.value),
  };
}

function renderScopeEstimateSummary() {
  const estimate = state.scopeEstimate;
  if (!estimate) {
    setScopeEstimateMessage("Select include tags to see approximate result count.");
    return;
  }

  const base = Number(estimate.base_total_count || 0);
  const estimated = Number(estimate.estimated_total_count || 0);
  const mode = String(estimate.estimate_mode || "exact");
  const requested = parseIntOrNull(refs.maxGames.value);
  const available = Number.isFinite(estimated) ? estimated : base;

  const parts = [];
  parts.push(`${formatInt(base)} matches by selected tags`);

  if (mode === "sampled") {
    const checked = Number(estimate.sample_checked || 0);
    const kept = Number(estimate.sample_kept || 0);
    parts.push(
      `~${formatInt(estimated)} after app type filters (sampled ${formatInt(checked)}, kept ${formatInt(kept)})`
    );
  } else if ((estimate.use_type_filter && Number(estimate.search_category1 || 0) > 0) || estimate.use_type_filter) {
    parts.push(`${formatInt(estimated)} after app type filters`);
  }

  let cssMode = "ok";
  if (requested !== null && requested > available) {
    parts.push(`requested ${formatInt(requested)}, available about ${formatInt(available)}; crawl will stop earlier without error`);
    cssMode = "warn";
  }

  setScopeEstimateMessage(`${parts.join(". ")}.`, cssMode);
}

async function fetchScopeEstimate() {
  if (state.estimateAbort) {
    state.estimateAbort.abort();
    state.estimateAbort = null;
  }

  const includeTags = [...state.includeTags];
  if (includeTags.length === 0) {
    state.scopeEstimate = null;
    renderScopeEstimateSummary();
    return;
  }

  const payload = buildEstimatePayload();
  const controller = new AbortController();
  state.estimateAbort = controller;
  setScopeEstimateMessage("Estimating result size...", "loading");

  try {
    const data = await fetchJSON("/api/crawl/estimate", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
      signal: controller.signal,
    });
    if (state.estimateAbort !== controller) return;
    state.scopeEstimate = data;
    renderScopeEstimateSummary();
  } catch (err) {
    if (controller.signal.aborted) return;
    state.scopeEstimate = null;
    setScopeEstimateMessage(`Estimate unavailable: ${err.message}`, "error");
  } finally {
    if (state.estimateAbort === controller) {
      state.estimateAbort = null;
    }
  }
}

function scheduleScopeEstimateRefresh(immediate = false) {
  if (state.estimateTimer) {
    clearTimeout(state.estimateTimer);
    state.estimateTimer = null;
  }
  if (immediate) {
    fetchScopeEstimate();
    return;
  }
  state.estimateTimer = setTimeout(() => {
    state.estimateTimer = null;
    fetchScopeEstimate();
  }, 450);
}

function setNumericInputValue(el, value) {
  el.value = value === null || value === undefined ? "" : String(value);
}

function resetFormToDefaults() {
  const d = state.defaults || {};
  refs.datasetDir.value = d.dataset_dir ?? "steam_dataset";
  setNumericInputValue(refs.maxGames, d.max_games ?? null);
  setNumericInputValue(refs.searchPageSize, d.search_page_size ?? 50);
  setNumericInputValue(refs.sleepSearch, d.sleep_search ?? 0.1);
  setNumericInputValue(refs.sleepReviews, d.sleep_reviews ?? 0.25);
  setNumericInputValue(refs.maxReviewPages, d.max_review_pages_per_game ?? 1);
  setNumericInputValue(refs.timeout, d.timeout ?? 30);
  refs.resume.checked = Boolean(d.resume ?? true);
  refs.enrichAppdetails.checked = Boolean(d.enrich_appdetails ?? true);
  refs.storeRawAppdetails.checked = Boolean(d.store_raw_appdetails ?? true);
  refs.excludeComingSoon.checked = Boolean(d.exclude_coming_soon ?? true);
  setNumericInputValue(refs.workers, d.workers ?? 12);
  setNumericInputValue(refs.maxRpm, d.max_rpm ?? 800);
  setNumericInputValue(refs.httpRetries, d.http_retries ?? 6);
  setNumericInputValue(refs.retryBaseSleep, d.http_retry_base_sleep ?? 1.0);
  setNumericInputValue(refs.retryJitter, d.http_retry_jitter ?? 0.6);
  setNumericInputValue(refs.maxAppRetries, d.max_app_retries ?? 4);
  setNumericInputValue(refs.appreviewsPageSize, d.appreviews_num_per_page ?? 100);

  state.includeTags = new Set(dedupeArray(d.tags || []));
  state.excludeTags = new Set(dedupeArray(d.exclude_tags || []));
  state.includeTypes = new Set(dedupeArray(d.include_app_types || []).map((x) => x.toLowerCase()));
  state.excludeTypes = new Set(dedupeArray(d.exclude_app_types || []).map((x) => x.toLowerCase()));
  refs.reviewSource.value = d.review_source ?? "appreviews";

  refs.tagRuleSearch.value = "";
  renderTagRuleOptions();
  renderTagRuleChips();
  refs.typeRuleSearch.value = "";
  renderTypeRuleOptions();
  renderTypeRuleChips();
  updatePayloadPreview();
  state.scopeEstimate = null;
  scheduleScopeEstimateRefresh(true);
}

function collectPayload() {
  const payload = {
    tags: [...state.includeTags],
    exclude_tags: [...state.excludeTags],
    dataset_dir: refs.datasetDir.value.trim(),
    max_games: parseIntOrNull(refs.maxGames.value),
    search_page_size: parseIntOrNull(refs.searchPageSize.value),
    sleep_search: parseFloatOrNull(refs.sleepSearch.value),
    sleep_reviews: parseFloatOrNull(refs.sleepReviews.value),
    max_review_pages_per_game: parseIntOrNull(refs.maxReviewPages.value),
    timeout: parseIntOrNull(refs.timeout.value),
    resume: refs.resume.checked,
    enrich_appdetails: refs.enrichAppdetails.checked,
    store_raw_appdetails: refs.storeRawAppdetails.checked,
    exclude_coming_soon: refs.excludeComingSoon.checked,
    workers: parseIntOrNull(refs.workers.value),
    max_rpm: parseIntOrNull(refs.maxRpm.value),
    http_retries: parseIntOrNull(refs.httpRetries.value),
    http_retry_base_sleep: parseFloatOrNull(refs.retryBaseSleep.value),
    http_retry_jitter: parseFloatOrNull(refs.retryJitter.value),
    max_app_retries: parseIntOrNull(refs.maxAppRetries.value),
    review_source: refs.reviewSource.value,
    appreviews_num_per_page: parseIntOrNull(refs.appreviewsPageSize.value),
    include_app_types: [...state.includeTypes],
    exclude_app_types: [...state.excludeTypes],
  };

  const compact = {};
  for (const [key, value] of Object.entries(payload)) {
    if (value === null || value === undefined) continue;
    if (Array.isArray(value) && value.length === 0) continue;
    if (typeof value === "string" && !value.trim()) continue;
    compact[key] = value;
  }
  return compact;
}

function updatePayloadPreview() {
  if (!refs.payloadPreview) return;
  const payload = collectPayload();
  refs.payloadPreview.textContent = JSON.stringify(payload, null, 2);
}

function clearPolling() {
  if (state.pollTimer) {
    clearInterval(state.pollTimer);
    state.pollTimer = null;
  }
  if (state.estimateTimer) {
    clearTimeout(state.estimateTimer);
    state.estimateTimer = null;
  }
  if (state.estimateAbort) {
    state.estimateAbort.abort();
    state.estimateAbort = null;
  }
}

function setCurrentJob(job) {
  const progress = Number(job.progress ?? 0);
  const status = String(job.status || "unknown");
  refs.jobIdValue.textContent = job.id || "-";
  refs.jobStatusValue.textContent = status;
  refs.jobProgressValue.textContent = formatPercent(progress);
  refs.jobProgressFill.style.width = formatPercent(progress);
  refs.jobMessage.textContent = job.message || "No message";
  if (refs.openResultBtn) {
    refs.openResultBtn.disabled = status !== "completed";
  }
  refs.cancelJobBtn.disabled = !(status === "running" || status === "queued");
}

async function fetchJSON(url, options = {}) {
  const resp = await fetch(url, options);
  const data = await resp.json().catch(() => ({}));
  if (!resp.ok) {
    const message = data.detail || JSON.stringify(data);
    throw new Error(message);
  }
  return data;
}

async function refreshJobsList() {
  try {
    const data = await fetchJSON("/api/jobs");
    const jobs = Array.isArray(data.jobs) ? data.jobs : [];
    refs.jobsList.innerHTML = "";
    if (jobs.length === 0) {
      refs.jobsList.textContent = "No jobs yet.";
      return;
    }
    const fragment = document.createDocumentFragment();
    for (const job of jobs.slice(0, 10)) {
      const row = document.createElement("div");
      row.className = "job-row";
      const status = String(job.status || "queued");

      const statusPill = document.createElement("span");
      statusPill.className = `job-status-pill ${status}`;
      statusPill.textContent = status;

      const body = document.createElement("div");
      body.className = "job-body";
      const summary = summarizeJobForList(job);

      const main = document.createElement("p");
      main.className = "job-main";
      main.textContent = summary.main;

      const sub = document.createElement("p");
      sub.className = "job-sub";
      sub.textContent = summary.sub;

      body.appendChild(main);
      body.appendChild(sub);

      const useBtn = document.createElement("button");
      useBtn.type = "button";
      useBtn.className = "btn-secondary";
      useBtn.textContent = "Watch";
      useBtn.title = `Job ID: ${String(job.id || "")}`;
      useBtn.addEventListener("click", () => {
        state.currentJobId = job.id;
        pollJobStatus(job.id, true);
      });

      row.appendChild(statusPill);
      row.appendChild(body);
      row.appendChild(useBtn);
      fragment.appendChild(row);
    }
    refs.jobsList.appendChild(fragment);
  } catch (err) {
    refs.jobsList.textContent = `Cannot load jobs: ${err.message}`;
  }
}

async function pollJobStatus(jobId, immediate = false) {
  if (!jobId) return;
  state.currentJobId = jobId;
  let lastStatus = "";

  const pollOnce = async () => {
    try {
      const data = await fetchJSON(`/api/jobs/${jobId}`);
      setCurrentJob(data);
      lastStatus = String(data.status || "");
      if (data.status === "completed" || data.status === "failed" || data.status === "cancelled") {
        clearPolling();
        refs.startCrawlBtn.disabled = false;
        refs.cancelJobBtn.disabled = true;
        await refreshJobsList();
      }
    } catch (err) {
      refs.jobMessage.textContent = `Polling error: ${err.message}`;
      clearPolling();
      refs.startCrawlBtn.disabled = false;
      refs.cancelJobBtn.disabled = true;
    }
  };

  if (immediate) {
    await pollOnce();
  }

  if (lastStatus === "completed" || lastStatus === "failed" || lastStatus === "cancelled") {
    return;
  }

  clearPolling();
  state.pollTimer = setInterval(pollOnce, 1000);
}

async function startCrawlJob() {
  const payload = collectPayload();
  if (!payload.tags || payload.tags.length === 0) {
    refs.jobMessage.textContent = "Select at least one INCLUDE tag before start.";
    return;
  }
  refs.startCrawlBtn.disabled = true;
  refs.cancelJobBtn.disabled = true;
  if (refs.openResultBtn) {
    refs.openResultBtn.disabled = true;
  }
  refs.jobMessage.textContent = "Submitting crawl job...";

  try {
    const data = await fetchJSON("/api/crawl/start", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });
    if (!data.job_id) {
      throw new Error("Backend response does not include job_id");
    }
    state.currentJobId = data.job_id;
    if (refs.jobResultPreview) {
      refs.jobResultPreview.textContent = "{}";
    }
    await pollJobStatus(data.job_id, true);
  } catch (err) {
    refs.jobMessage.textContent = `Start failed: ${err.message}`;
    refs.startCrawlBtn.disabled = false;
    refs.cancelJobBtn.disabled = true;
  }
}

async function cancelCurrentJob() {
  if (!state.currentJobId) return;
  refs.cancelJobBtn.disabled = true;
  refs.jobMessage.textContent = "Sending cancel request...";
  try {
    await fetchJSON(`/api/jobs/${state.currentJobId}/cancel`, { method: "POST" });
    await pollJobStatus(state.currentJobId, true);
  } catch (err) {
    refs.jobMessage.textContent = `Cancel failed: ${err.message}`;
  }
}

async function loadJobResult() {
  if (!refs.jobResultPreview) return;
  if (!state.currentJobId) return;
  try {
    const data = await fetchJSON(`/api/jobs/${state.currentJobId}/result`);
    refs.jobResultPreview.textContent = JSON.stringify(data.result || data, null, 2);
  } catch (err) {
    refs.jobResultPreview.textContent = JSON.stringify({ detail: err.message }, null, 2);
  }
}

function bindEvents() {
  refs.layoutSidebarBtn.addEventListener("click", () => applyLayout("sidebar"));
  refs.layoutTopBtn.addEventListener("click", () => applyLayout("top"));

  for (const tip of document.querySelectorAll(".help-tip")) {
    tip.setAttribute("tabindex", "0");
    tip.addEventListener("click", (event) => {
      event.preventDefault();
      event.stopPropagation();
    });
  }

  refs.tagRuleSearch.addEventListener("input", renderTagRuleOptions);
  refs.typeRuleSearch.addEventListener("input", renderTypeRuleOptions);
  refs.timeout.addEventListener("change", () => scheduleScopeEstimateRefresh());
  refs.searchPageSize.addEventListener("change", () => scheduleScopeEstimateRefresh());
  refs.maxGames.addEventListener("input", renderScopeEstimateSummary);
  refs.maxGames.addEventListener("change", renderScopeEstimateSummary);

  refs.crawlForm.addEventListener("submit", async (event) => {
    event.preventDefault();
    await startCrawlJob();
  });

  refs.resetFormBtn.addEventListener("click", resetFormToDefaults);
  refs.refreshJobsBtn.addEventListener("click", refreshJobsList);
  refs.cancelJobBtn.addEventListener("click", cancelCurrentJob);
  if (refs.openResultBtn) {
    refs.openResultBtn.addEventListener("click", loadJobResult);
  }

  const reactiveInputs = [
    refs.datasetDir,
    refs.maxGames,
    refs.searchPageSize,
    refs.sleepSearch,
    refs.sleepReviews,
    refs.maxReviewPages,
    refs.timeout,
    refs.resume,
    refs.enrichAppdetails,
    refs.storeRawAppdetails,
    refs.excludeComingSoon,
    refs.workers,
    refs.maxRpm,
    refs.httpRetries,
    refs.retryBaseSleep,
    refs.retryJitter,
    refs.maxAppRetries,
    refs.reviewSource,
    refs.appreviewsPageSize,
  ];
  for (const input of reactiveInputs) {
    input.addEventListener("input", updatePayloadPreview);
    input.addEventListener("change", updatePayloadPreview);
  }
}

function mountRefs() {
  refs.backendBadge = byId("backendBadge");
  refs.apiBase = byId("apiBase");
  refs.layoutSidebarBtn = byId("layoutSidebarBtn");
  refs.layoutTopBtn = byId("layoutTopBtn");
  refs.mainLayout = byId("mainLayout");

  refs.crawlForm = byId("crawlForm");
  refs.selectedTagRules = byId("selectedTagRules");
  refs.tagRuleSearch = byId("tagRuleSearch");
  refs.tagRuleOptions = byId("tagRuleOptions");
  refs.scopeEstimateHint = byId("scopeEstimateHint");
  refs.selectedTypeRules = byId("selectedTypeRules");
  refs.typeRuleSearch = byId("typeRuleSearch");
  refs.typeRuleOptions = byId("typeRuleOptions");

  refs.datasetDir = byId("datasetDir");
  refs.maxGames = byId("maxGames");
  refs.searchPageSize = byId("searchPageSize");
  refs.sleepSearch = byId("sleepSearch");
  refs.sleepReviews = byId("sleepReviews");
  refs.maxReviewPages = byId("maxReviewPages");
  refs.timeout = byId("timeout");
  refs.resume = byId("resume");
  refs.enrichAppdetails = byId("enrichAppdetails");
  refs.storeRawAppdetails = byId("storeRawAppdetails");
  refs.excludeComingSoon = byId("excludeComingSoon");
  refs.workers = byId("workers");
  refs.maxRpm = byId("maxRpm");
  refs.httpRetries = byId("httpRetries");
  refs.retryBaseSleep = byId("retryBaseSleep");
  refs.retryJitter = byId("retryJitter");
  refs.maxAppRetries = byId("maxAppRetries");
  refs.reviewSource = byId("reviewSource");
  refs.appreviewsPageSize = byId("appreviewsPageSize");

  refs.payloadPreview = byId("payloadPreview");
  refs.jobIdValue = byId("jobIdValue");
  refs.jobStatusValue = byId("jobStatusValue");
  refs.jobProgressValue = byId("jobProgressValue");
  refs.jobProgressFill = byId("jobProgressFill");
  refs.jobMessage = byId("jobMessage");
  refs.jobsList = byId("jobsList");
  refs.jobResultPreview = byId("jobResultPreview");

  refs.startCrawlBtn = byId("startCrawlBtn");
  refs.resetFormBtn = byId("resetFormBtn");
  refs.refreshJobsBtn = byId("refreshJobsBtn");
  refs.cancelJobBtn = byId("cancelJobBtn");
  refs.openResultBtn = byId("openResultBtn");
}

function fillReviewSourceOptions() {
  refs.reviewSource.innerHTML = "";
  for (const item of state.reviewSources) {
    const option = document.createElement("option");
    option.value = item.value;
    option.textContent = item.label;
    refs.reviewSource.appendChild(option);
  }
}

async function bootstrap() {
  try {
    const data = await fetchJSON("/api/bootstrap");
    state.defaults = data.defaults || {};
    state.tags = Array.isArray(data.tags) ? data.tags : [];
    state.appTypes = Array.isArray(data.app_type_options) ? data.app_type_options : [];
    state.reviewSources = Array.isArray(data.review_sources) ? data.review_sources : [];

    refs.apiBase.textContent = data.api_base_url || refs.apiBase.textContent;
    fillReviewSourceOptions();
    resetFormToDefaults();
    await refreshJobsList();
    setBackendBadge("ok", "Backend is online");
  } catch (err) {
    setBackendBadge("error", "Backend unavailable");
    refs.jobMessage.textContent = `Bootstrap failed: ${err.message}`;
    if (refs.payloadPreview) {
      refs.payloadPreview.textContent = JSON.stringify({ detail: err.message }, null, 2);
    }
  }
}

function init() {
  mountRefs();
  bindEvents();
  applyLayout("sidebar");
  bootstrap();
}

window.addEventListener("beforeunload", clearPolling);
window.addEventListener("DOMContentLoaded", init);
