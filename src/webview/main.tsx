import React from "react";
import { createRoot } from "react-dom/client";
import { App } from "./components/App";
import "@vscode/codicons/dist/codicon.css";

const vscode = acquireVsCodeApi();

window.__vscode = vscode;

const initialStateFromHost = window.__RAPIDB_INITIAL_STATE__;
const persistedState = vscode.getState<{
  initialState?: typeof initialStateFromHost;
}>();

if (!initialStateFromHost && persistedState?.initialState) {
  window.__RAPIDB_INITIAL_STATE__ = persistedState.initialState;
}

if (window.__RAPIDB_INITIAL_STATE__) {
  vscode.setState({
    initialState: window.__RAPIDB_INITIAL_STATE__,
  });
}

const rootEl = document.getElementById("root");
if (!rootEl) {
  throw new Error("[RapiDB] #root element not found");
}

createRoot(rootEl).render(<App />);
