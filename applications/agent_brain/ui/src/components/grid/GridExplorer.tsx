import {
  useCallback,
  useEffect,
  useMemo,
  useState,
} from "react";
import { ExternalLink, Star } from "lucide-react";
import { useFavoritesStore } from "@/store/favorites";
import { api } from "@/lib/api";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { cn } from "@/components/ui/cn";

interface GridExplorerProps {
  topicId: string | null;
  className?: string;
}

export function GridExplorer({ topicId, className }: GridExplorerProps) {
  const { isFavorite, toggleFavorite } = useFavoritesStore();
  const [columns, setColumns] = useState<string[]>([]);
  const [data, setData] = useState<any[]>([]);
  const [total, setTotal] = useState(0);
  const [page, setPage] = useState(1);
  const [pageSize, setPageSize] = useState(20);
  const [sortKey, setSortKey] = useState<string | null>(null);
  const [sortDir, setSortDir] = useState<"asc" | "desc">("asc");
  const [filter, setFilter] = useState("");

  const fetchData = useCallback(async () => {
    if (!topicId) return;
    try {
      const res = await api.getPages(topicId, { page, page_size: pageSize });
      const pages = res.pages as any[];
      setTotal(res.total || 0);
      setData(pages);

      if (pages.length > 0 && columns.length === 0) {
        const keys = Object.keys(pages[0]);
        setColumns(keys);
      }
    } catch (err) {
      console.error("Failed to load pages", err);
    }
  }, [topicId, page, pageSize, columns.length]);

  useEffect(() => {
    fetchData();
  }, [fetchData]);

  const favoriteIds = useMemo(
    () => new Set(data.map((d) => d.page_id).filter((id) => isFavorite(id))),
    [data, isFavorite]
  );

  const sortedFiltered = useMemo(() => {
    let result = [...data];

    if (filter) {
      const f = filter.toLowerCase();
      result = result.filter((row) =>
        Object.values(row).some(
          (v) => v != null && String(v).toLowerCase().includes(f)
        )
      );
    }

    if (sortKey) {
      result.sort((a, b) => {
        const av = a[sortKey];
        const bv = b[sortKey];
        let cmp = 0;
        if (av < bv) cmp = -1;
        else if (av > bv) cmp = 1;
        return sortDir === "desc" ? -cmp : cmp;
      });
    }

    const pinned = result.filter((d) => favoriteIds.has(d.page_id));
    const unpinned = result.filter((d) => !favoriteIds.has(d.page_id));
    return [...pinned, ...unpinned];
  }, [data, filter, sortKey, sortDir, favoriteIds]);

  const totalPages = Math.ceil(total / pageSize);

  const labelFor = (key: string) =>
    key
      .replace(/_/g, " ")
      .replace(/\b\w/g, (c) => c.toUpperCase());

  return (
    <div className={cn("flex flex-col h-full", className)}>
      <div className="flex items-center justify-between gap-4 mb-4">
        <div className="flex items-center gap-2 text-sm font-medium">
          Topic: {topicId ?? "None selected"}
        </div>
        <div className="flex items-center gap-2">
          <Input
            placeholder="Filter columns…"
            value={filter}
            onChange={(e) => setFilter(e.target.value)}
            className="w-48 text-sm"
          />
        </div>
      </div>

      <div className="flex-1 relative overflow-hidden rounded-xl border border-border bg-background shadow-sm">
        <div className="overflow-x-auto">
          <table className="w-full text-sm text-left">
            <thead>
              <tr className="border-b border-border bg-surface/50 sticky top-0 z-10">
                <th className="px-4 py-3 text-xs font-medium text-muted-foreground uppercase tracking-wider sticky left-0 z-20 bg-background">
                  Favorite
                </th>
                {columns.map((key) => (
                  <th
                    key={key}
                    className={cn(
                      "px-4 py-3 text-xs font-medium text-muted-foreground uppercase tracking-wider cursor-pointer select-none hover:bg-accent/50",
                      sortKey === key && "text-primary font-semibold"
                    )}
                    onClick={() => {
                      if (sortKey === key) {
                        setSortDir(sortDir === "asc" ? "desc" : "asc");
                      } else {
                        setSortKey(key);
                        setSortDir("asc");
                      }
                    }}
                  >
                    <div className="flex items-center gap-1">
                      {labelFor(key)}
                      {sortKey === key && (
                        <span className="text-xs text-muted-foreground">
                          {sortDir === "asc" ? "▲" : "▼"}
                        </span>
                      )}
                    </div>
                  </th>
                ))}
              </tr>
            </thead>
            <tbody>
              {sortedFiltered.map((row) => {
                const pageId = row.page_id;
                const isFav = favoriteIds.has(pageId);
                return (
                  <tr
                    key={pageId}
                    className={cn(
                      "border-b border-border hover:bg-accent/50 transition-colors",
                      isFav && "bg-amber-50 border-l-2 border-amber-500"
                    )}
                  >
                    <td className="px-4 py-3 w-12 sticky left-0 z-20 bg-background">
                      <button
                        onClick={() => toggleFavorite(pageId)}
                        className={cn(
                          "w-6 h-6 rounded-full flex items-center justify-center transition-colors",
                          isFav
                            ? "text-yellow-500 bg-amber-100"
                            : "text-muted-foreground hover:text-primary hover:bg-primary/10"
                        )}
                        aria-label={isFav ? "Unpin" : "Pin"}
                      >
                        <Star className="h-4 w-4 fill-current" />
                      </button>
                    </td>
                    {Object.entries(row).map(([key, val]) => {
                      if (key === "page_id" || key === "topic_id" || key === "text")
                        return null;
                      const strVal = val == null ? "" : String(val);
                      return (
                        <td key={key} className="px-4 py-3 text-sm break-words">
                          {key === "url" ? (
                            <a
                              href={strVal}
                              target="_blank"
                              rel="noopener noreferrer"
                              className="text-primary underline-offset-4 hover:underline"
                            >
                              {strVal.length > 60 ? strVal.slice(0, 60) + "…" : strVal}
                              <ExternalLink
                                className="ml-1 h-3 w-3 fill-current"
                                aria-label="Open in new tab"
                              />
                            </a>
                          ) : (
                            <span className="text-foreground">
                              {strVal || "—"}
                            </span>
                          )}
                        </td>
                      );
                    })}
                  </tr>
                );
              })}
              {sortedFiltered.length === 0 && (
                <tr>
                  <td
                    colSpan={3}
                    className="px-4 py-8 text-center text-sm text-muted-foreground"
                  >
                    No data found
                  </td>
                </tr>
              )}
            </tbody>
          </table>
        </div>

        <div className="absolute bottom-0 left-0 w-full border-t border-border bg-background/95 backdrop-blur p-2">
          <div className="flex items-center justify-between gap-4 px-4 py-2">
            <div className="flex items-center gap-2">
              <Button
                variant="outline"
                size="sm"
                onClick={() => setPage((p) => Math.max(1, p - 1))}
                disabled={page === 1}
              >
                <span>←</span>
              </Button>
              <Button
                variant="outline"
                size="sm"
                onClick={() => setPage((p) => p + 1)}
                disabled={page * pageSize >= total}
              >
                <span>→</span>
              </Button>
            </div>
            <div className="text-xs text-muted-foreground">
              Page {page} of {totalPages}
            </div>
            <div className="flex items-center gap-2">
              <span className="text-xs text-muted-foreground">Rows per page:</span>
              {[20, 50, 100].map((size) => (
                <Button
                  key={size}
                  variant="outline"
                  size="sm"
                  onClick={() => setPageSize(size)}
                  className={cn(
                    "h-8 w-8",
                    pageSize === size
                      ? "bg-primary text-primary-foreground"
                      : ""
                  )}
                >
                  {size}
                </Button>
              ))}
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}
