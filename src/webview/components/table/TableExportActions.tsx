import React from "react";
import { Icon } from "../Icon";
import { tableButtonStyle } from "./tableViewHelpers";

export type ExportFormat = "csv" | "json";

interface TableExportActionsProps {
  disabled?: boolean;
  onExport: (format: ExportFormat) => void;
  titleByFormat?: Partial<Record<ExportFormat, string>>;
  buttonStyle?: (disabled: boolean) => React.CSSProperties;
  gap?: number;
  iconSize?: number;
  iconMarginRight?: number;
}

const EXPORT_ACTIONS: ReadonlyArray<{
  format: ExportFormat;
  label: string;
}> = [
  { format: "csv", label: "Export CSV" },
  { format: "json", label: "Export JSON" },
];

export function TableExportActions({
  disabled = false,
  onExport,
  titleByFormat,
  buttonStyle,
  gap = 4,
  iconSize = 13,
  iconMarginRight = 4,
}: TableExportActionsProps) {
  const resolveButtonStyle =
    buttonStyle ??
    ((isDisabled: boolean) => tableButtonStyle("ghost", isDisabled));

  return (
    <div style={{ display: "flex", gap }}>
      {EXPORT_ACTIONS.map(({ format, label }) => (
        <button
          key={format}
          type="button"
          style={resolveButtonStyle(disabled)}
          disabled={disabled}
          onClick={() => onExport(format)}
          title={titleByFormat?.[format]}
        >
          <Icon
            name="export"
            size={iconSize}
            style={{ marginRight: iconMarginRight }}
          />
          {label}
        </button>
      ))}
    </div>
  );
}
