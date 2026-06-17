// Stub for the vscode module so that the bundler does not try to resolve
// the real "vscode" package (which is only available inside the VS Code
// extension host). Real mocks live in tests/workflow/bridge/workflowVscode.ts
// and are installed via vi.mock at the top of each workflow test file.

export {};
