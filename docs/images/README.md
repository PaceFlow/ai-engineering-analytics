# README dashboard screenshots

These images are direct captures of the complete xterm window running
`vca tui --all-projects`. They include the app header, navigation tabs, grouping
and time-window controls, report body, and keyboard footer. The legend image
shows the app's own overlay. No report values were edited or reconstructed.

Captured September 11, 2026, using an isolated copy of the engineer's private
September 10 Cursor validation database, with the 90-day window selected.
The user authorized inclusion of these images, including visible branch names.
The source database and raw report exports are not included in the repository.

| Image | View |
| --- | --- |
| `tui-verdict.png` | Verdict, grouped by model |
| `tui-sessions.png` | Sessions, grouped by model |
| `tui-sessions-branches.png` | Sessions, grouped by branch |
| `tui-delivery.png` | Delivery, grouped by model |
| `tui-quality.png` | Quality, grouped by model |
| `tui-legend.png` | Quality metric legend |

## Capture setup

- Terminal: xterm, 120 columns × 36 rows, DejaVu Sans Mono 14, 16-pixel border.
- Background: `#09090b`; foreground: `#f4f4f5`.
- Native application colors enabled by unsetting `NO_COLOR`.
- The app ran inside tmux with true-color support on an isolated Xvfb display.
- ImageMagick `import -window <xterm-window-id> <output.png>` captured the entire window.

To refresh, run the current binary against an isolated analytics database, launch
`vca tui --all-projects`, press `w` to select 90 days, and use `1`–`4` to capture
each tab. On Sessions, cycle `g` to branch grouping; on Quality, press `L` for
the legend. Review the full images and their data before replacing these assets.
Keep captions accurate about the capture date, provider coverage, and scope.
