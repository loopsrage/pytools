import { useEffect, useRef, useState } from "react";
import { ChevronDown, X } from "lucide-react";
import { useTopicState } from "@/store/topics";
import { useFavoritesStore } from "@/store/favorites";
import { api } from "@/lib/api";
import { Input } from "@/components/ui/input";
import { Badge } from "@/components/ui/badge";
import { cn } from "@/components/ui/cn";

interface TopicDropdownProps {
  className?: string;
  onTopicSelect: (topicId: string) => void;
}

const STATUS_VARIANTS: Record<string, string> = {
  active: "bg-green-500/20 text-green-700 border-green-100",
  paused: "bg-amber-500/20 text-amber-700 border-amber-100",
  blocked: "bg-red-500/20 text-red-700 border-red-100",
};

export function TopicDropdown({ className, onTopicSelect }: TopicDropdownProps) {
  const { topics, setTopics, selectedTopicId } = useTopicState();
  const { isFavorite } = useFavoritesStore();
  const [isOpen, setIsOpen] = useState(false);
  const [search, setSearch] = useState("");
  const [loading, setLoading] = useState(false);
  const inputRef = useRef<HTMLInputElement>(null);

  useEffect(() => {
    loadTopics();
  }, []);

  async function loadTopics() {
    setLoading(true);
    try {
      const res = await api.getTopics({ page_size: 100 });
      setTopics(res.topics as any[]);
    } catch (err) {
      console.error("Failed to load topics", err);
    } finally {
      setLoading(false);
    }
  }

  const filtered = topics.filter(
    (t) =>
      t.name.toLowerCase().includes(search.toLowerCase()) ||
      t.url.toLowerCase().includes(search.toLowerCase())
  );

  return (
    <div className={cn("relative w-full max-w-md", className)}>
      <div className="relative">
        <Input
          ref={inputRef}
          placeholder="Select a topic…"
          value={search}
          onChange={(e) => setSearch(e.target.value)}
          onFocus={() => setIsOpen(true)}
          onBlur={() => setTimeout(() => setIsOpen(false), 150)}
          className="pr-10"
        />
        {isOpen ? (
          <button
            onClick={(e) => {
              e.stopPropagation();
              setSearch("");
              setIsOpen(false);
            }}
            className="absolute right-2 top-2 p-1 text-muted-foreground hover:text-foreground transition-colors"
            aria-label="Clear search"
          >
            <X className="h-4 w-4" />
          </button>
        ) : (
          <ChevronDown className="absolute right-3 top-3 h-4 w-4 text-muted-foreground" />
        )}
      </div>

      {isOpen && !loading && (
        <div className="mt-1 z-50 w-full bg-background border border-border rounded-xl shadow-lg overflow-hidden animate-in fade-in zoom-in-95 duration-200">
          <div className="px-3 py-2 border-b border-border bg-background/50">
            <span className="text-xs font-semibold uppercase tracking-wider text-muted-foreground">
              Topics ({filtered.length})
            </span>
          </div>
          <div className="max-h-96 overflow-y-auto">
            {filtered.length === 0 ? (
              <div className="px-3 py-4 text-center text-sm text-muted-foreground">
                No topics found
              </div>
            ) : (
              filtered.map((topic) => {
                const isActive = selectedTopicId === topic.topic_id;
                return (
                  <button
                    key={topic.topic_id}
                    onClick={() => {
                      onTopicSelect(topic.topic_id);
                      setIsOpen(false);
                    }}
                    className={cn(
                      "w-full flex items-center gap-3 px-3 py-2.5 text-sm transition-colors",
                      isActive
                        ? "bg-primary/10 text-primary border-r-2 border-primary"
                        : "hover:bg-accent/50 border-transparent"
                    )}
                  >
                    <Badge
                      variant="default"
                      className={cn(
                        STATUS_VARIANTS[topic.status] || "bg-secondary text-secondary-foreground",
                        "w-[8px] h-[8px] rounded-full px-0 py-0 text-[10px]"
                      )}
                    />
                    <div className="flex-1 min-w-0">
                      <div className="truncate font-medium">{topic.name}</div>
                      <div className="truncate text-xs text-muted-foreground">{topic.url}</div>
                    </div>
                    {isFavorite(topic.topic_id) && (
                      <span className="text-warning">★</span>
                    )}
                  </button>
                );
              })
            )}
          </div>
          {loading && (
            <div className="p-2 text-xs text-muted-foreground text-center">Loading…</div>
          )}
        </div>
      )}
    </div>
  );
}
