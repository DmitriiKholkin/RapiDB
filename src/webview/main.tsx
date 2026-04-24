import React from "react";
import { createRoot } from "react-dom/client";
import { App } from "./components/App";
import "@vscode/codicons/dist/codicon.css";

const vscode = acquireVsCodeApi();

window.__vscode = vscode;

const rootEl = document.getElementById("root");
if (!rootEl) {
  throw new Error("[RapiDB] #root element not found");
}

createRoot(rootEl).render(<App />);
