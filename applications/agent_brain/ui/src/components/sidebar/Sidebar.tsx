import React, { useState } from "react";
import {
  Brain,
  Cpu,
  Database,
  FileCode2,
  LayoutDashboard,
  Palette,
  Settings,
  Sparkles,
  Trophy,
  X,
} from "lucide-react";
import { useThemeStore } from "@/store/theme";
import { Button } from "@/components/ui/button";
import { ColorWheel } from "@/components/theme/ColorWheel";
import { cn } from "@/components/ui/cn";

export interface SidebarItem {
  id: string;
  label: string;
  icon: React.ElementType;
}

export const NAV_ITEMS: SidebarItem[] = [
  { id: "explorer", label: "Explorer", icon: LayoutDashboard },
  { id: "brain", label: "Brain", icon: Brain },
  { id: "artifacts", label: "Artifacts", icon: FileCode2 },
  { id: "promoted-model", label: "Promoted Model", icon: Sparkles },
  { id: "benchmarks", label: "Model Benchmarks", icon: Trophy },
  { id: "staging-qdrant", label: "Staging Qdrant", icon: Database },
  { id: "crawlers", label: "Crawlers", icon: Cpu },
  { id: "settings", label: "Settings", icon: Settings },
  { id: "theme", label: "Theme", icon: Palette },
];

interface SidebarProps {
  className?: string;
  isOpen: boolean;
  onClose: () => void;
  activeSection: string;
  onSectionChange: (section: string) => void;
}

export function Sidebar({
  className,
  isOpen,
  onClose,
  activeSection,
  onSectionChange,
}: SidebarProps) {
  const [colorWheelOpen, setColorWheelOpen] = useState(false);
  const { theme, setMode } = useThemeStore();

  return (
    <>
      {isOpen && (
        <div
          className="fixed inset-0 z-40 bg-black/40 backdrop-blur-sm lg:hidden"
          onClick={onClose}
          aria-hidden="true"
        />
      )}
      <aside
        className={cn(
          "fixed inset-y-0 left-0 z-50 w-64 shrink-0 flex flex-col bg-background border-r border-border shadow-lg lg:shadow-none transform transition-transform duration-300 ease-in-out lg:static lg:translate-x-0",
          isOpen ? "translate-x-0" : "-translate-x-full",
          className
        )}
        aria-label="Sidebar"
      >
        <div className="flex items-center justify-between h-14 px-4 border-b border-border">
          <div className="flex items-center gap-2">
            <div className="h-8 w-8 rounded-xl bg-primary text-primary-foreground flex items-center justify-center font-bold text-sm">
              AB
            </div>
            <span className="font-bold text-lg tracking-tight">Agent Brain</span>
          </div>
          <button
            onClick={onClose}
            className="p-1 text-muted-foreground hover:text-foreground transition-colors lg:hidden"
            aria-label="Close sidebar"
          >
            <X className="h-5 w-5" />
          </button>
        </div>

        <div className="flex-1 overflow-y-auto p-3 space-y-2">
          <nav className="space-y-1">
            {NAV_ITEMS.map((item) => {
              const isActive = activeSection === item.id;
              return (
                <button
                  key={item.id}
                  data-testid={`nav-item-${item.id}`}
                  onClick={() => onSectionChange(item.id)}
                  className={cn(
                    "w-full flex items-center gap-3 px-3 py-2 rounded-lg text-sm font-medium transition-colors text-left",
                    isActive
                      ? "bg-primary text-primary-foreground"
                      : "text-muted-foreground hover:bg-accent/50 hover:text-foreground"
                  )}
                >
                  <item.icon className="h-4 w-4 shrink-0" />
                  <span className="truncate">{item.label}</span>
                </button>
              );
            })}
          </nav>

          <div className="mt-4 pt-4 border-t border-border">
            <div className="flex items-center gap-2 mb-3">
              <span className="text-xs font-semibold uppercase tracking-wider text-muted-foreground">
                Appearance
              </span>
              <Button
                variant="ghost"
                size="icon"
                onClick={() => setColorWheelOpen(true)}
                className="h-8 w-8"
                aria-label="Open theme color wheel"
              >
                <Palette className="h-4 w-4" />
              </Button>
            </div>
            <div className="flex gap-2">
              {(["light", "dark", "system"] as const).map((mode) => (
                <button
                  key={mode}
                  onClick={() => setMode(mode)}
                  className={cn(
                    "h-8 w-8 rounded-full flex items-center justify-center transition-colors",
                    theme.mode === mode
                      ? "bg-primary text-primary-foreground"
                      : "bg-secondary text-secondary-foreground"
                  )}
                  aria-label={`Theme mode: ${mode}`}
                >
                  {mode === "light" ? "☀" : mode === "dark" ? "☾" : "↻"}
                </button>
              ))}
            </div>
          </div>
        </div>

        <div className="p-3 border-t border-border">
          <Button
            variant="outline"
            className="w-full"
            onClick={() => onSectionChange("crawlers")}
          >
            <Cpu className="h-4 w-4 mr-2" />
            Crawler Status
          </Button>
        </div>
      </aside>

      {colorWheelOpen && (
        <ColorWheel isOpen={colorWheelOpen} onClose={() => setColorWheelOpen(false)} />
      )}
    </>
  );
}
