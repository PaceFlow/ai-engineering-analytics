//! Rendering for the observational workflow; data is supplied by the shared query layer.
use super::{
    app::{App, Screen},
    data::Data,
};
use crate::insights::trends::Report;
use ratatui::{
    Frame,
    layout::{Constraint, Layout, Rect},
    text::Line,
    widgets::{Paragraph, Row, Wrap},
};
fn n(v: Option<f64>) -> String {
    v.map(|v| format!("{v:.1}")).unwrap_or_else(|| "—".into())
}
fn p(v: Option<f64>) -> String {
    v.map(|v| format!("{:.0}%", v * 100.0))
        .unwrap_or_else(|| "—".into())
}
pub fn row_count(r: &Report, tab: usize) -> usize {
    match tab {
        0 => r.periods.len(),
        1 => r
            .periods
            .iter()
            .map(|p| p.goal_mix.len() + p.activity_mix.len())
            .sum(),
        2 => r.comparison.as_ref().map_or(0, |c| c.deltas.len()),
        _ => r.milestones.len() + r.observations.len(),
    }
}
pub fn contributors(r: &Report, tab: usize, index: usize) -> Vec<String> {
    match tab {
        0 => r
            .periods
            .get(index)
            .map(|p| {
                p.contributing_work_ids
                    .iter()
                    .chain(p.count_work_ids.values().flatten())
                    .cloned()
                    .collect::<std::collections::BTreeSet<_>>()
                    .into_iter()
                    .collect()
            })
            .unwrap_or_default(),
        1 => r
            .periods
            .iter()
            .flat_map(|p| p.goal_mix.iter().chain(&p.activity_mix))
            .nth(index)
            .map(|m| m.work_ids.clone())
            .unwrap_or_default(),
        2 => r
            .comparison
            .as_ref()
            .map(|c| {
                let metric = c.deltas.get(index).map(|d| d.metric.as_str()).unwrap_or("");
                let ids = |p: &crate::insights::trends::Period| match metric {
                    "cycle_minutes" => p.cycle_minutes.work_ids.clone(),
                    "active_minutes" => p.active_minutes.work_ids.clone(),
                    "tokens_per_completed" => p.tokens_per_completed.work_ids.clone(),
                    "conversations_per_completed" => p.conversations.work_ids.clone(),
                    "started" | "completed" | "open" | "abandoned" => {
                        p.count_work_ids.get(metric).cloned().unwrap_or_default()
                    }
                    _ => p.contributing_work_ids.clone(),
                };
                ids(&c.before)
                    .into_iter()
                    .chain(ids(&c.after))
                    .collect::<std::collections::BTreeSet<_>>()
                    .into_iter()
                    .collect()
            })
            .unwrap_or_default(),
        _ => vec![],
    }
}
pub fn render(frame: &mut Frame<'_>, app: &App, area: Rect) {
    let chunks = Layout::vertical([Constraint::Length(4), Constraint::Min(3)]).split(area);
    let selected = Some(app.frame().selected);
    let Some(data) = &app.frame().data else {
        return;
    };
    match data {
        Data::Daily(r) => {
            frame.render_widget(Paragraph::new(vec![Line::from(format!("Daily work · {} · {}",r.timezone,r.scope.repo.as_deref().unwrap_or("all projects"))),Line::from("Completion, usage, stages and corrections remain work-item metrics. PR/deployment evidence is additional."),Line::from(r.warnings.first().cloned().unwrap_or_else(||"Tab: views · Enter: contributing work · x: evidence".into()))]).wrap(Wrap{trim:true}),chunks[0]);
            let rows = r
                .days
                .iter()
                .map(|d| {
                    Row::new(vec![
                        format!("{}{}", d.date, if d.partial { "*" } else { "" }),
                        d.started.to_string(),
                        d.active_work.to_string(),
                        d.completed.to_string(),
                        d.bug_work_active.to_string(),
                        d.native_tokens.total.to_string(),
                        d.corrections.to_string(),
                        format!("{}/{}", d.pull_requests.merged, d.pull_requests.deployed),
                        d.models
                            .iter()
                            .max_by_key(|m| m.usage_events)
                            .map(|m| m.model.clone())
                            .unwrap_or_else(|| "—".into()),
                    ])
                })
                .collect();
            super::ui::render_table(
                frame,
                app,
                chunks[1],
                &[
                    "Day",
                    "Start",
                    "Active",
                    "Done",
                    "Bug work",
                    "Tokens",
                    "Corrections",
                    "Merged/live",
                    "Leading model",
                ],
                &[
                    Constraint::Length(12),
                    Constraint::Length(6),
                    Constraint::Length(6),
                    Constraint::Length(6),
                    Constraint::Length(8),
                    Constraint::Length(12),
                    Constraint::Length(11),
                    Constraint::Length(12),
                    Constraint::Min(12),
                ],
                rows,
                selected,
            );
        }
        Data::Trends(r) => {
            let tab = if let Screen::Trends { tab, .. } = app.frame().screen {
                tab
            } else {
                0
            };
            let title = ["Timeline", "Work mix", "Comparison", "Setup changes"][tab % 4];
            frame.render_widget(Paragraph::new(vec![Line::from(format!("{title} · {} · {}",r.timezone,r.scope.repo.as_deref().unwrap_or("all projects"))),Line::from("Open includes completion-unknown work. Samples below 10 are small; — means unavailable."),Line::from(r.warnings.first().cloned().unwrap_or_else(||"Enter opens the contributing work; Tab changes view.".into()))]).wrap(Wrap{trim:true}),chunks[0]);
            match tab {
                0 => {
                    let rows = r
                        .periods
                        .iter()
                        .map(|p| {
                            Row::new(vec![
                                format!("{}{}", p.from, if p.provisional { " *" } else { "" }),
                                p.started.to_string(),
                                p.completed.to_string(),
                                p.open.to_string(),
                                p.abandoned.to_string(),
                                n(p.cycle_minutes.median),
                                n(p.cycle_minutes.p90),
                                n(p.tokens_per_completed.median),
                                format!(
                                    "{}/{}",
                                    p.cycle_minutes.samples, p.tokens_per_completed.samples
                                ),
                                format!(
                                    "{} / {}",
                                    p.coverage.missing_usage, p.coverage.unclassified_intents
                                ),
                            ])
                        })
                        .collect();
                    super::ui::render_table(
                        frame,
                        app,
                        chunks[1],
                        &[
                            "Week",
                            "Start",
                            "Done",
                            "Open",
                            "Drop",
                            "Cycle min",
                            "p90",
                            "Tokens/task",
                            "n time/token",
                            "Missing/labels",
                        ],
                        &[
                            Constraint::Length(12),
                            Constraint::Length(5),
                            Constraint::Length(5),
                            Constraint::Length(5),
                            Constraint::Length(5),
                            Constraint::Length(10),
                            Constraint::Length(9),
                            Constraint::Length(13),
                            Constraint::Length(12),
                            Constraint::Min(10),
                        ],
                        rows,
                        selected,
                    );
                }
                1 => {
                    let rows = r
                        .periods
                        .iter()
                        .flat_map(|period| {
                            period
                                .goal_mix
                                .iter()
                                .map(move |m| (period, "goal", m))
                                .chain(
                                    period
                                        .activity_mix
                                        .iter()
                                        .map(move |m| (period, "activity", m)),
                                )
                        })
                        .map(|(period, kind, m)| {
                            Row::new(vec![
                                period.from.clone(),
                                kind.into(),
                                m.label.clone(),
                                m.work_items.to_string(),
                                p(m.work_share),
                                format!("{:.1}", m.activity_minutes),
                                p(m.activity_share),
                                m.native_tokens.to_string(),
                                p(m.token_share),
                            ])
                        })
                        .collect();
                    super::ui::render_table(
                        frame,
                        app,
                        chunks[1],
                        &[
                            "Week",
                            "Basis",
                            "Label",
                            "Work",
                            "Share",
                            "Minutes",
                            "Share",
                            "Native tokens",
                            "Share",
                        ],
                        &[
                            Constraint::Length(11),
                            Constraint::Length(9),
                            Constraint::Min(20),
                            Constraint::Length(6),
                            Constraint::Length(7),
                            Constraint::Length(10),
                            Constraint::Length(7),
                            Constraint::Length(14),
                            Constraint::Length(7),
                        ],
                        rows,
                        selected,
                    );
                }
                2 => {
                    if let Some(c) = &r.comparison {
                        let rows = c
                            .deltas
                            .iter()
                            .map(|d| {
                                Row::new(vec![
                                    d.metric.clone(),
                                    n(d.before),
                                    n(d.after),
                                    n(d.absolute),
                                    d.unit.clone(),
                                    d.note.clone().unwrap_or_else(|| {
                                        d.relative_percent
                                            .map(|v| format!("{v:+.1}%"))
                                            .unwrap_or_default()
                                    }),
                                    format!("{}/{}", d.before_samples, d.after_samples),
                                ])
                            })
                            .collect();
                        super::ui::render_table(
                            frame,
                            app,
                            chunks[1],
                            &[
                                "Metric",
                                "Before",
                                "After",
                                "Change",
                                "Unit",
                                "Relative",
                                "n before/after",
                            ],
                            &[
                                Constraint::Min(25),
                                Constraint::Length(12),
                                Constraint::Length(12),
                                Constraint::Length(12),
                                Constraint::Length(18),
                                Constraint::Length(14),
                                Constraint::Length(14),
                            ],
                            rows,
                            selected,
                        );
                    } else {
                        frame.render_widget(Paragraph::new("No comparison available"), chunks[1]);
                    }
                }
                _ => {
                    let rows = r
                        .milestones
                        .iter()
                        .map(|m| {
                            Row::new(vec![
                                format!("#{}", m.id),
                                m.at.clone(),
                                m.title.clone(),
                                "manual milestone".into(),
                            ])
                        })
                        .chain(r.observations.iter().map(|o| {
                            Row::new(vec![
                                o.state.clone(),
                                o.effective_at
                                    .as_deref()
                                    .unwrap_or(&o.observed_at)
                                    .to_string(),
                                o.identity.clone(),
                                if o.effective_at.is_some() {
                                    "session evidence"
                                } else {
                                    "observed now; effective time unknown"
                                }
                                .into(),
                            ])
                        }))
                        .collect();
                    super::ui::render_table(
                        frame,
                        app,
                        chunks[1],
                        &["Change", "Time", "Setup / title", "Evidence"],
                        &[
                            Constraint::Length(15),
                            Constraint::Length(25),
                            Constraint::Min(25),
                            Constraint::Length(35),
                        ],
                        rows,
                        selected,
                    );
                }
            }
        }
        Data::WorkItems(items) => {
            frame.render_widget(Paragraph::new(format!("{} work items · Enter opens the goal and contributing conversations\nL relationships · u cycles list units",items.len())),chunks[0]);
            let rows = items
                .iter()
                .map(|w| {
                    Row::new(vec![
                        w.id[..12].into(),
                        w.state.clone(),
                        w.intent.clone(),
                        w.conversations.to_string(),
                        n(w.cycle_minutes()),
                        w.native_tokens()
                            .map(|v| v.to_string())
                            .unwrap_or("—".into()),
                        w.title.clone(),
                    ])
                })
                .collect();
            super::ui::render_table(
                frame,
                app,
                chunks[1],
                &[
                    "ID",
                    "State",
                    "Intent",
                    "Chats",
                    "Cycle min",
                    "Native tokens",
                    "Goal",
                ],
                &[
                    Constraint::Length(12),
                    Constraint::Length(10),
                    Constraint::Length(18),
                    Constraint::Length(5),
                    Constraint::Length(10),
                    Constraint::Length(14),
                    Constraint::Min(20),
                ],
                rows,
                selected,
            );
        }
        Data::WorkDetail(Some(w)) => {
            frame.render_widget(Paragraph::new(format!("{}\n{} · {} · {} conversations · cycle {} min · work activity {:.1} min\n{} completion signals · {} reopenings · {} PRs / {} live · L relationships · x evidence",w.title,w.id,w.state,w.conversations,n(w.cycle_minutes()),w.active_minutes(),w.completions.len(),w.reopens.len(),w.pull_requests.len(),w.pull_requests.iter().filter(|p|p.deployed_at.is_some()).count())).wrap(Wrap{trim:true}),chunks[0]);
            let rows = w
                .members
                .iter()
                .map(|m| {
                    Row::new(vec![
                        m.id.clone(),
                        format!(
                            "{}:{}#{}",
                            m.task.provider, m.task.session_id, m.task.task_index
                        ),
                        m.task.outcome.clone().unwrap_or("unknown".into()),
                        m.review_reason.clone().unwrap_or_default(),
                    ])
                })
                .collect();
            super::ui::render_table(
                frame,
                app,
                chunks[1],
                &["Member ID", "Conversation task", "Outcome", "Needs review"],
                &[
                    Constraint::Length(32),
                    Constraint::Min(25),
                    Constraint::Length(22),
                    Constraint::Min(15),
                ],
                rows,
                selected,
            );
        }
        Data::WorkDetail(None) => frame.render_widget(
            Paragraph::new("Work identity changed; return to the work list."),
            area,
        ),
        Data::WorkLinks(rows) => {
            frame.render_widget(Paragraph::new("Only accepted continuation/reopen links join work items.\na accepts · d rejects · l adds a manual link · x shows evidence"),chunks[0]);
            let values = rows
                .iter()
                .map(|r| {
                    Row::new(vec![
                        r.id[..12].into(),
                        r.kind.clone(),
                        r.decision.clone(),
                        r.source.clone(),
                        r.from_id[..12].into(),
                        r.to_id[..12].into(),
                        r.review_reason
                            .clone()
                            .unwrap_or_else(|| r.evidence.clone()),
                    ])
                })
                .collect();
            super::ui::render_table(
                frame,
                app,
                chunks[1],
                &[
                    "ID", "Relation", "Decision", "Source", "From", "To", "Evidence",
                ],
                &[
                    Constraint::Length(12),
                    Constraint::Length(23),
                    Constraint::Length(10),
                    Constraint::Length(18),
                    Constraint::Length(12),
                    Constraint::Length(12),
                    Constraint::Min(15),
                ],
                values,
                selected,
            );
        }
        _ => {}
    }
}
