import { useCallback, useEffect, useState } from "react";
import { Play, RefreshCw, Bot, Clock, Link, Terminal } from "lucide-react";
import { api } from "@/lib/api";
import { Button } from "@/components/ui/button";
import { cn } from "@/components/ui/cn";

interface CrawlerControlProps {
  className?: string;
}

export function CrawlerControl({ className }: CrawlerControlProps) {
  const [health, setHealth] = useState<any>(null);
  const [loading, setLoading] = useState(false);

  useEffect(() => {
    loadHealth();
    const interval = setInterval(loadHealth, 5000);
    return () => clearInterval(interval);
  }, []);

  const loadHealth = useCallback(async () => {
    try {
      const res = await api.health();
      setHealth(res);
    } catch (err) {
      console.error("Health check failed", err);
    }
  }, []);

  async function scrapeAll() {
    setLoading(true);
    try {
      await api.scrapeAll();
      await loadHealth();
    } catch (err) {
      console.error("Scrape all failed", err);
    } finally {
      setLoading(false);
    }
  }

  return (
    <div className={cn("flex flex-col gap-4 h-full", className)}>
      <div className="flex items-center gap-2">
        <Bot className="h-5 w-5 text-muted-foreground" />
        <span className="text-sm font-semibold">Crawler Control</span>
      </div>

      <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
        <StatCard title="Registered Topics" value={health?.topics ?? 0} icon={<Clock className="h-4 w-4" />} />
        <StatCard title="Service Status" value={health?.status ?? "unknown"} icon={<Link className="h-4 w-4" />} />
      </div>

      <div className="grid grid-cols-1 sm:grid-cols-2 gap-3">
        <Button
          onClick={scrapeAll}
          disabled={loading}
          className="w-full flex items-center justify-center gap-2"
        >
          <Play className="h-4 w-4" />
          {loading ? "Scraping…" : "Scrape All Due"}
        </Button>
        <Button
          variant="outline"
          onClick={loadHealth}
          disabled={loading}
          className="w-full flex items-center justify-center gap-2"
        >
          <RefreshCw className={cn("h-4 w-4", loading && "animate-spin")} />
          <span>Refresh</span>
        </Button>
      </div>

      <div className="mt-2 p-3 rounded-md bg-muted/50 border border-border/50">
        <div className="flex items-center gap-2 mb-1">
          <Terminal className="h-4 w-4 text-muted-foreground" />
          <span className="text-xs font-medium">Crawler API</span>
        </div>
        <div className="text-xs text-muted-foreground space-y-1">
          <div>GET /topics — list registered topics</div>
          <div>{"POST /topics/{id}/scrape — scrape a single topic"}</div>
          <div>{"POST /topics/{id}/crawl — crawl job links on a topic"}</div>
          <div>POST /scrape-all — scrape all due topics</div>
        </div>
      </div>
    </div>
  );
}

function StatCard({ title, value, icon }: { title: string; value: any; icon: React.ReactNode }) {
  return (
    <div className="p-3 rounded-xl border border-border bg-background">
      <div className="flex items-center justify-between mb-1">
        <span className="text-xs text-muted-foreground">{title}</span>
        {icon}
      </div>
      <div className="text-lg font-semibold">{String(value)}</div>
    </div>
  );
}
