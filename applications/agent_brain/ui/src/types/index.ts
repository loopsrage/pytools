export interface Topic {
  topic_id: string;
  name: string;
  url: string;
  status: string;
  scrape_interval_seconds: number;
  last_scraped: string | null;
  robots_txt_allowed: boolean;
  created_at: string;
  metadata: Record<string, unknown>;
}

export interface Page {
  page_id: string;
  topic_id: string;
  url: string;
  url_hash: string | null;
  title: string | null;
  content_hash: string | null;
  text: string | null;
  stored_path: string | null;
  scraped_at: string;
}

export interface Artifact {
  artifact_id: string;
  topic_id: string;
  page_id: string | null;
  kind: string;
  stored_path: string;
  row_count: number | null;
  created_at: string;
}

export interface FavoriteTopic {
  topic_id: string;
  pinned_at: string;
}

export interface PaginatedResponse<T> {
  data: T[];
  total: number;
  page: number;
  page_size: number;
  total_pages: number;
}

export interface GridColumn {
  key: string;
  label: string;
}
