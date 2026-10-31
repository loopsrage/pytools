export type ThemeMode = "light" | "dark" | "system";

export interface ThemeColors {
  primary: string;
  secondary: string;
  accent: string;
  background: string;
  surface: string;
  text: string;
  textMuted: string;
  border: string;
  success: string;
  warning: string;
  destructive: string;
}

export interface ThemeConfig {
  mode: ThemeMode;
  colors: ThemeColors;
  radius: number;
  fontFamily: string;
}

export const DEFAULT_THEME: ThemeConfig = {
  mode: "system",
  colors: {
    primary: "#0f172a",
    secondary: "#f1f5f9",
    accent: "#3b82f6",
    background: "#ffffff",
    surface: "#f8fafc",
    text: "#0f172a",
    textMuted: "#64748b",
    border: "#e2e8f0",
    success: "#10b981",
    warning: "#f59e0b",
    destructive: "#ef4444",
  },
  radius: 8,
  fontFamily: "system-ui, -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif",
};
