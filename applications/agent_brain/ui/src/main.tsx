import React from "react";
import ReactDOM from "react-dom/client";
import App from "./App";
import "./style.css";
import { useThemeStore } from "@/store/theme";

ReactDOM.createRoot(document.getElementById("root")!).render(
  <React.StrictMode>
    <App />
  </React.StrictMode>
);

// Apply theme on startup
useThemeStore.getState().applyTheme();
