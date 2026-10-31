import { useEffect, useState } from "react";
import { Save, RotateCw, FileText, Terminal } from "lucide-react";
import { api } from "@/lib/api";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Card, CardHeader, CardTitle, CardContent, CardDescription } from "@/components/ui/card";
import { cn } from "@/components/ui/cn";
import { ResumeUploader } from "./ResumeUploader";

interface SettingsEditorProps {
  className?: string;
}

export function SettingsEditor({ className }: SettingsEditorProps) {
  const [yaml, setYaml] = useState("");
  const [loading, setLoading] = useState(false);
  const [saving, setSaving] = useState(false);
  const [saved, setSaved] = useState(false);

  useEffect(() => {
    loadSettings();
  }, []);

  async function loadSettings() {
    setLoading(true);
    try {
      const res = await api.getSettingsYaml();
      setYaml(res.yaml || "");
    } catch (err) {
      console.error("Failed to load settings", err);
    } finally {
      setLoading(false);
    }
  }

  async function saveSettings() {
    setSaving(true);
    try {
      await api.updateSettings({ settings: yaml });
      setSaved(true);
      setTimeout(() => setSaved(false), 2000);
    } catch (err) {
      console.error("Failed to save settings", err);
    } finally {
      setSaving(false);
    }
  }

  return (
    <div className={cn("flex flex-col gap-4 h-full", className)}>
      <div className="flex items-center justify-between gap-4">
        <div className="flex items-center gap-2">
          <FileText className="h-5 w-5 text-muted-foreground" />
          <span className="text-sm font-semibold">settings.yaml</span>
        </div>
        <div className="flex items-center gap-2">
          <Badge variant={saved ? "secondary" : "default"}>
            {saved ? "Saved" : "Unsaved changes"}
          </Badge>
          <Button
            variant="outline"
            size="sm"
            onClick={loadSettings}
            disabled={loading}
            className="flex items-center gap-1"
          >
            <RotateCw className={cn("h-4 w-4", loading && "animate-spin")} />
            <span>Reload</span>
          </Button>
          <Button
            onClick={saveSettings}
            disabled={saving || !saved}
            className="flex items-center gap-1"
          >
            <Save className={cn("h-4 w-4", saving && "animate-spin")} />
            <span>Save</span>
          </Button>
        </div>
      </div>

      <div className="flex-1 relative rounded-xl border border-border overflow-hidden shadow-sm">
        <textarea
          value={yaml}
          onChange={(e) => {
            setYaml(e.target.value);
            setSaved(false);
          }}
          className="w-full h-full p-4 font-mono text-sm text-foreground bg-background border-0 focus-visible:outline-none resize-none"
          spellCheck={false}
          disabled={loading}
          placeholder="Loading settings…"
        />
        {loading && (
          <div className="absolute inset-0 bg-background/80 backdrop-blur-sm flex items-center justify-center">
            <span className="text-sm text-muted-foreground">Loading…</span>
          </div>
        )}
      </div>

      <div className="flex items-center justify-between text-xs text-muted-foreground pt-2 border-t border-border">
        <span>
          Edit the agent brain configuration directly. Changes require a service
          restart to take effect.
        </span>
        <div className="flex items-center gap-2">
          <Terminal className="h-3 w-3" />
          <span>uv run uvicorn applications.agent_brain.main:app --reload --port 8127</span>
        </div>
      </div>

      <Card>
        <CardHeader>
          <CardTitle>Resume Attachment</CardTitle>
          <CardDescription>
            Upload a resume (PDF/DOC/DOCX/TXT) that will be attached to job
            application emails when jobs are approved for application.
          </CardDescription>
        </CardHeader>
        <CardContent>
          <ResumeUploader onResumeUploaded={() => setSaved(false)} />
        </CardContent>
      </Card>
    </div>
  );
}
