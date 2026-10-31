"use client";

import * as React from "react";
import { Upload, FileText, AlertCircle, RefreshCw } from "lucide-react";
import { cn } from "@/components/ui/cn";
import { Button } from "@/components/ui/button";
import { api } from "@/lib/api";
import { useResumeStore } from "@/store/resume";

interface ResumeUploaderProps {
  className?: string;
  acceptedTypes?: string[];
  maxSizeMB?: number;
  onResumeUploaded?: (path: string) => void;
}

const DEFAULT_ACCEPTED_TYPES = [".pdf", ".doc", ".docx", ".txt"];
const DEFAULT_MAX_SIZE_MB = 10;

export function ResumeUploader({
  className,
  acceptedTypes = DEFAULT_ACCEPTED_TYPES,
  maxSizeMB = DEFAULT_MAX_SIZE_MB,
  onResumeUploaded,
}: ResumeUploaderProps) {
  const [dragActive, setDragActive] = React.useState(false);
  const [uploading, setUploading] = React.useState(false);
  const [error, setError] = React.useState<string | null>(null);
  const resume = useResumeStore((s) => s.resume);
  const setResume = useResumeStore((s) => s.setResume);
  const clearResume = useResumeStore((s) => s.clearResume);

  const isValidFile = (file: File): string | null => {
    if (!acceptedTypes.some((type) => file.name.toLowerCase().endsWith(type.toLowerCase()))) {
      return `Unsupported file type. Allowed: ${acceptedTypes.join(", ")}`;
    }
    const maxSizeBytes = maxSizeMB * 1024 * 1024;
    if (file.size > maxSizeBytes) {
      return `File too large. Maximum size is ${maxSizeMB} MB.`;
    }
    return null;
  };

  const handleDrop = React.useCallback(
    async (e: React.DragEvent) => {
      e.preventDefault();
      setDragActive(false);
      const file = e.dataTransfer.files[0];
      if (file) await uploadFile(file);
    },
    []
  );

  const handleDragOver = React.useCallback((e: React.DragEvent) => {
    e.preventDefault();
    setDragActive(true);
  }, []);

  const handleDragLeave = React.useCallback((e: React.DragEvent) => {
    e.preventDefault();
    setDragActive(false);
  }, []);

  const handleFileSelect = React.useCallback(
    async (e: React.ChangeEvent<HTMLInputElement>) => {
      const file = e.target.files?.[0];
      if (file) await uploadFile(file);
      e.target.value = "";
    },
    []
  );

  const uploadFile = React.useCallback(
    async (file: File) => {
      const validationError = isValidFile(file);
      if (validationError) {
        setError(validationError);
        setTimeout(() => setError(null), 5000);
        return;
      }

      setUploading(true);
      setError(null);
      try {
        const res = await api.uploadResume(file);
        setResume(res.path);
        onResumeUploaded?.(res.path);
      } catch (err) {
        setError(err instanceof Error ? err.message : "Failed to upload resume");
      } finally {
        setUploading(false);
      }
    },
    [onResumeUploaded, setResume, isValidFile]
  );

  const removeResume = React.useCallback(() => {
    clearResume();
    setError(null);
  }, [clearResume]);

  return (
    <div className={cn("flex flex-col gap-4", className)}>
      <section
        onDrop={handleDrop}
        onDragOver={handleDragOver}
        onDragLeave={handleDragLeave}
        className={cn(
          "flex flex-col items-center justify-center rounded-xl border-2 border-dashed p-8 text-center transition-all duration-200",
          dragActive
            ? "border-primary bg-accent/50 text-primary"
            : "border-border bg-background text-muted-foreground hover:border-primary/50",
        )}
      >
        <div className={cn("flex flex-col items-center gap-3", dragActive ? "animate-pulse" : "")}>
          <Upload className="h-10 w-10" />
          <span className="text-sm font-medium">
            {dragActive ? "Drop your resume here" : "Drop your resume here or click to browse"}
          </span>
          <p className="text-xs text-muted-foreground">
            {acceptedTypes.join(", ").toUpperCase()} • Max {maxSizeMB} MB
          </p>
        </div>
        <input
          type="file"
          accept={acceptedTypes.join(",")}
          className="sr-only"
          onChange={handleFileSelect}
        />
        <label className="mt-4 cursor-pointer">
          <Button variant="outline" size="sm">
            {uploading ? (
              <>
                <RefreshCw className="h-3 w-3 animate-spin mr-2" />
                Uploading…
              </>
            ) : (
              <><Upload className="h-3 w-3 mr-2" /> Browse files</>
            )}
          </Button>
        </label>
      </section>

      {error && (
        <div className="rounded-lg border border-destructive/50 bg-destructive/10 p-4 flex items-center gap-3 text-destructive text-sm">
          <AlertCircle className="h-4 w-4 shrink-0" />
          <span>{error}</span>
        </div>
      )}

      {resume && (
        <div className="rounded-lg border bg-secondary/50 p-4 flex items-center justify-between gap-3">
          <div className="flex items-center gap-3">
            <FileText className="h-4 w-4 text-muted-foreground shrink-0" />
            <div>
              <p className="text-sm font-medium truncate" title={resume}>
                {resume}
              </p>
              <p className="text-xs text-muted-foreground">
                Current resume attached to job application emails
              </p>
            </div>
          </div>
          <Button variant="outline" size="sm" onClick={removeResume}>
            Remove
          </Button>
        </div>
      )}
    </div>
  );
}
