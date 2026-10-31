import { useState } from "react";
import { BrowserRouter, Routes, Route, useNavigate, useLocation } from "react-router-dom";
import { ArrowLeft } from "lucide-react";
import { Sidebar } from "@/components/sidebar/Sidebar";
import { TopicDropdown } from "@/components/dropdown/TopicDropdown";
import { GridExplorer } from "@/components/grid/GridExplorer";
import { Button } from "@/components/ui/button";
import { SettingsEditor } from "@/components/settings/SettingsEditor";
import { CrawlerControl } from "@/components/crawler/CrawlerControl";

function AppRouter() {
  return (
    <BrowserRouter>
      <Routes>
        <Route path="/" element={<Dashboard />} />
        <Route path="/settings" element={<SettingsPage />} />
        <Route path="/crawlers" element={<CrawlersPage />} />
        <Route path="/theme" element={<ThemePage />} />
      </Routes>
    </BrowserRouter>
  );
}

function Dashboard() {
  const [topicId, setTopicId] = useState<string | null>(null);
  const [sidebarOpen, setSidebarOpen] = useState(false);
  const navigate = useNavigate();
  const location = useLocation();

  const activeSection = location.pathname === "/" ? "explorer" : location.pathname.slice(1);

  return (
    <div className="flex h-screen flex-col lg:flex-row overflow-hidden bg-background">
      <Sidebar
        isOpen={sidebarOpen}
        onClose={() => setSidebarOpen(false)}
        activeSection={activeSection}
        onSectionChange={(section) => {
          setSidebarOpen(false);
          navigate(`/${section}`);
        }}
      />

      <div className="flex-1 flex flex-col min-w-0 relative">
        <header className="flex items-center justify-between h-14 px-4 border-b border-border bg-background/80 backdrop-blur sticky top-0 z-20">
          <div className="flex items-center gap-3">
            <button
              onClick={() => setSidebarOpen(true)}
              className="p-2 text-muted-foreground hover:text-foreground transition-colors lg:hidden"
              aria-label="Toggle sidebar"
            >
              <div className="w-5 h-0.5 bg-current"></div>
              <div className="w-5 h-0.5 bg-current mt-1"></div>
              <div className="w-5 h-0.5 bg-current mt-1"></div>
            </button>
            {topicId ? (
              <button
                onClick={() => setTopicId(null)}
                className="flex items-center gap-1.5 text-sm font-medium"
                aria-label="Back to topics"
              >
                <ArrowLeft className="h-4 w-4" />
                <span>Back to topics</span>
              </button>
            ) : (
              <div className="flex items-center gap-2">
                <span className="text-sm font-semibold">Agent Brain Control Panel</span>
                <div className="h-5 w-px bg-border" />
              </div>
            )}
          </div>

          <div className="flex items-center gap-2">
            {!topicId ? (
              <TopicDropdown onTopicSelect={setTopicId} className="w-full max-w-sm" />
            ) : (
              <Button variant="outline" size="sm" onClick={() => setTopicId(null)}>
                <ArrowLeft className="h-3 w-3" />
                Back
              </Button>
            )}
          </div>
        </header>

        <main className="flex-1 overflow-y-auto p-4 lg:p-6">
          {topicId ? (
            <GridExplorer topicId={topicId} />
          ) : (
            <TopicsList />
          )}
        </main>
      </div>
    </div>
  );
}

function TopicsList() {
  return (
    <div className="space-y-4">
      <div className="flex items-center justify-between">
        <h1 className="text-2xl font-bold">Topics</h1>
        <p className="text-muted-foreground">Manage and explore your agent brain topics.</p>
      </div>
      <div className="rounded-xl border border-border bg-background p-6">
        <p className="text-sm text-muted-foreground">
          Select a topic from the dropdown above to open its paginated data explorer.
        </p>
      </div>
    </div>
  );
}

function SettingsPage() {
  return (
    <div className="h-full p-6">
      <h1 className="text-2xl font-bold mb-4">Settings</h1>
      <SettingsEditor />
    </div>
  );
}

function CrawlersPage() {
  return (
    <div className="h-full p-6">
      <h1 className="text-2xl font-bold mb-4">Crawler Control</h1>
      <CrawlerControl />
    </div>
  );
}

function ThemePage() {
  return (
    <div className="h-full p-6">
      <h1 className="text-2xl font-bold mb-4">Theme</h1>
      <p className="text-muted-foreground">Customize the control panel appearance.</p>
    </div>
  );
}

export default AppRouter;
