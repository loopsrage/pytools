import React, { useCallback } from "react";
import { X } from "lucide-react";
import { useThemeStore } from "@/store/theme";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { cn } from "@/components/ui/cn";

const COLOR_KEYS = [
  { key: "primary", label: "Primary" },
  { key: "secondary", label: "Secondary" },
  { key: "accent", label: "Accent" },
  { key: "background", label: "Background" },
  { key: "surface", label: "Surface" },
  { key: "text", label: "Text" },
  { key: "textMuted", label: "Text Muted" },
  { key: "border", label: "Border" },
  { key: "success", label: "Success" },
  { key: "warning", label: "Warning" },
  { key: "destructive", label: "Destructive" },
] as const;

interface ColorWheelProps {
  className?: string;
  isOpen: boolean;
  onClose: () => void;
}

export function ColorWheel({ className, isOpen, onClose }: ColorWheelProps) {
  const { theme, setColor, setMode, setRadius, resetTheme } = useThemeStore();

  const handleInput = useCallback(
    (e: React.ChangeEvent<HTMLInputElement>, key: keyof typeof theme.colors) => {
      setColor(key, e.target.value);
    },
    [setColor]
  );

  return (
    <div className={cn("fixed inset-0 z-50 flex items-center justify-center bg-black/50 backdrop-blur-sm", className)}>
      {!isOpen ? null : (
        <>
          <div
            className="absolute inset-0"
            onClick={onClose}
            aria-label="Close color wheel"
          />
          <div
            className="relative z-10 w-full max-w-md mx-4 bg-background rounded-xl border border-border shadow-xl p-6 animate-in fade-in slide-in-from-bottom-2 duration-300"
            role="dialog"
            aria-modal="true"
          >
            <div className="flex items-center justify-between mb-6">
              <h2 className="text-lg font-semibold">Theme Colors</h2>
              <Button
                variant="ghost"
                size="icon"
                onClick={onClose}
                aria-label="Close"
              >
                <X className="h-5 w-5" />
              </Button>
            </div>

            <div className="space-y-4">
              <div>
                <div className="flex items-center justify-between mb-2">
                  <span className="text-sm font-medium text-muted-foreground">Mode</span>
                  <div className="flex gap-2">
                    {["light", "dark", "system"].map((mode) => (
                      <Button
                        key={mode}
                        size="sm"
                        variant={theme.mode === mode ? "default" : "outline"}
                        onClick={() => setMode(mode as "light" | "dark" | "system")}
                        className={`h-8 w-8 ${theme.mode === mode ? "bg-primary text-primary-foreground" : ""}`}
                        aria-label={`Theme mode: ${mode}`}
                      >
                        {mode === "light" ? (
                          <span className="text-xs">☀</span>
                        ) : mode === "dark" ? (
                          <span className="text-xs">☾</span>
                        ) : (
                          <span className="text-xs">↻</span>
                        )}
                      </Button>
                    ))}
                  </div>
                </div>
              </div>

              <div className="grid grid-cols-2 gap-3">
                {COLOR_KEYS.map(({ key, label }) => {
                  const value = theme.colors[key];
                  return (
                    <div key={key} className="flex flex-col gap-1.5">
                      <div className="flex items-center justify-between">
                        <label className="text-xs font-medium text-muted-foreground">{label}</label>
                        <span className="text-xs text-muted-foreground">{value}</span>
                      </div>
                      <input
                        type="color"
                        value={value}
                        onChange={(e) => handleInput(e, key)}
                        className="h-8 w-full rounded-md border-0 bg-transparent cursor-pointer p-0"
                      />
                    </div>
                  );
                })}
              </div>

              <div className="flex items-center justify-between pt-4 border-t border-border">
                <div className="flex items-center gap-2">
                  <span className="text-xs font-medium text-muted-foreground">Radius</span>
                  <input
                    type="range"
                    min="0"
                    max="24"
                    step="1"
                    value={theme.radius}
                    onChange={(e) => setRadius(Number(e.target.value))}
                    className="w-24 accent-primary"
                  />
                  <Badge variant="secondary">{theme.radius}px</Badge>
                </div>
                <Button variant="outline" size="sm" onClick={resetTheme}>
                  Reset
                </Button>
              </div>
            </div>
          </div>
        </>
      )}
    </div>
  );
}
