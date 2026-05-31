import React from "react";
import { createRoot } from "react-dom/client";
import { App } from "./components/App";
import { syncInitialWebviewState } from "./utils/vscodeState";
import "@vscode/codicons/dist/codicon.css";

const vscode = acquireVsCodeApi();

window.__vscode = vscode;

const initialStateFromHost = window.__RAPIDB_INITIAL_STATE__;
syncInitialWebviewState(vscode, initialStateFromHost);

const rootEl = document.getElementById("root");
if (!rootEl) {
  throw new Error("[RapiDB] #root element not found");
}

createRoot(rootEl).render(<App />);
