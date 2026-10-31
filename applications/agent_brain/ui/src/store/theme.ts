import { create } from "zustand";
import { persist, createJSONStorage } from "zustand/middleware";
import { DEFAULT_THEME } from "@/types/theme";
import type { ThemeConfig, ThemeMode } from "@/types/theme";

interface ThemeStore {
  theme: ThemeConfig;
  setMode: (mode: ThemeMode) => void;
  setColor: (key: keyof ThemeConfig["colors"], value: string) => void;
  setRadius: (radius: number) => void;
  setFontFamily: (fontFamily: string) => void;
  resetTheme: () => void;
  applyTheme: () => void;
}

export const useThemeStore = create<ThemeStore>()(
  persist(
    (set, get) => ({
      theme: DEFAULT_THEME,

      setMode: (mode) => {
        set((state) => ({ theme: { ...state.theme, mode } }));
        get().applyTheme();
      },

      setColor: (key, value) => {
        set((state) => ({
          theme: {
            ...state.theme,
            colors: { ...state.theme.colors, [key]: value },
          },
        }));
        get().applyTheme();
      },

      setRadius: (radius) => {
        set((state) => ({ theme: { ...state.theme, radius } }));
        get().applyTheme();
      },

      setFontFamily: (fontFamily) => {
        set((state) => ({ theme: { ...state.theme, fontFamily } }));
        get().applyTheme();
      },

      resetTheme: () => {
        set({ theme: DEFAULT_THEME });
        get().applyTheme();
      },

      applyTheme: () => {
        const { theme } = get();
        const root = document.documentElement;

        root.setAttribute("data-theme", theme.mode === "system" ? (window.matchMedia("(prefers-color-scheme: dark)").matches ? "dark" : "light") : theme.mode);

        root.style.setProperty("--color-primary", theme.colors.primary);
        root.style.setProperty("--color-secondary", theme.colors.secondary);
        root.style.setProperty("--color-accent", theme.colors.accent);
        root.style.setProperty("--color-background", theme.colors.background);
        root.style.setProperty("--color-surface", theme.colors.surface);
        root.style.setProperty("--color-text", theme.colors.text);
        root.style.setProperty("--color-text-muted", theme.colors.textMuted);
        root.style.setProperty("--color-border", theme.colors.border);
        root.style.setProperty("--color-success", theme.colors.success);
        root.style.setProperty("--color-warning", theme.colors.warning);
        root.style.setProperty("--color-destructive", theme.colors.destructive);
        root.style.setProperty("--radius", `${theme.radius}px`);
        root.style.setProperty("--font-family", theme.fontFamily);
      },
    }),
    {
      name: "agent-brain-theme-storage",
      storage: createJSONStorage(() => localStorage),
    }
  )
);

// Initialize theme on module load
if (typeof window !== "undefined") {
  useThemeStore.getState().applyTheme();
}
