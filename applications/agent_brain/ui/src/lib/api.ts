const API_BASE = import.meta.env.VITE_API_BASE || "/api";

async function request<T>(
  path: string,
  opts: RequestInit = {}
): Promise<T> {
  const isFormData = opts.body instanceof FormData;
  const headers: Record<string, string> = {};
  if (!isFormData) headers["Content-Type"] = "application/json";
  Object.assign(headers, opts.headers);
  const res = await fetch(`${API_BASE}${path}`, {
    headers,
    ...opts,
  });
  if (!res.ok) {
    throw new Error(`API error ${res.status}: ${await res.text()}`);
  }
  return res.json();
}

export const api = {
  getTopics: (params?: { page?: number; page_size?: number; search?: string }) => {
    const qs = new URLSearchParams();
    if (params?.page) qs.set("page", String(params.page));
    if (params?.page_size) qs.set("page_size", String(params.page_size));
    if (params?.search) qs.set("search", params.search);
    return request<{ topics: any[]; total: number }>(`/topics?${qs.toString()}`);
  },
  getTopic: (id: string) => request<Topic>(`/topics/${id}`),
  getPages: (topicId: string, params?: { page?: number; page_size?: number }) => {
    const qs = new URLSearchParams();
    if (params?.page) qs.set("page", String(params.page));
    if (params?.page_size) qs.set("page_size", String(params.page_size));
    return request<{ pages: any[]; total: number }>(`/topics/${topicId}/pages?${qs.toString()}`);
  },
  getArtifacts: (topicId: string) => request<any[]>(`/topics/${topicId}/artifacts`),
  getSettings: () => request<any>("/settings"),
  getSettingsYaml: () => {
    return fetch(`${API_BASE}/settings/yaml`).then(async (res) => {
      if (!res.ok) throw new Error(`API error ${res.status}`);
      const text = await res.text();
      return { yaml: text };
    });
  },
  updateSettings: (settings: any) =>
    request(`/settings`, { method: "POST", body: JSON.stringify(settings) }),
  toggleTopicStatus: (id: string) =>
    request(`/topics/${id}/status`, { method: "POST" }),
  scrapeTopic: (id: string) => request(`/topics/${id}/scrape`, { method: "POST" }),
  crawlTopic: (id: string) => request(`/topics/${id}/crawl`, { method: "POST" }),
  scrapeAll: () => request("/scrape-all", { method: "POST" }),
  health: () => request<{ status: string; topics: number }>("/health"),
  uploadResume: (file: File) => {
    const formData = new FormData();
    formData.append("file", file);
    return request<{ filename: string; path: string; size: number }>("/settings/resume", {
      method: "POST",
      body: formData,
      headers: {},
    });
  },
  getResume: () =>
    fetch(`${API_BASE}/settings/resume`).then(async (res) => {
      if (!res.ok) return { exists: false };
      const text = await res.text();
      return { exists: true, path: text };
    }),
};

export type Topic = {
  topic_id: string;
  name: string;
  url: string;
  status: string;
  scrape_interval_seconds: number;
  last_scraped: string | null;
  robots_txt_allowed: boolean;
  created_at: string;
  metadata: Record<string, unknown>;
};
