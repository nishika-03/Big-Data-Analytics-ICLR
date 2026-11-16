"""Generate the charts in figures/ from the result tables in results/.

Run from the repository root:  python code/make_figures.py
Requires: pandas, matplotlib
"""
import pandas as pd
import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt

SURFACE, TEXT, TEXT2, GRID = "#fcfcfb", "#0b0b0b", "#52514e", "#e4e3df"
COLORS = ["#2a78d6", "#eb6834", "#1baf7a", "#eda100"]
R, F = "results/", "figures/"

plt.rcParams.update({
    "figure.facecolor": SURFACE, "axes.facecolor": SURFACE, "axes.edgecolor": GRID,
    "axes.labelcolor": TEXT2, "xtick.color": TEXT2, "ytick.color": TEXT2, "text.color": TEXT,
    "font.size": 11, "axes.spines.top": False, "axes.spines.right": False,
    "axes.grid": True, "grid.color": GRID, "grid.linewidth": 0.8, "axes.axisbelow": True,
    "axes.titlesize": 13, "axes.titleweight": "bold", "axes.titlelocation": "left",
})


def save(fig, name):
    fig.tight_layout()
    fig.savefig(F + name, dpi=160)
    plt.close(fig)


# Q1: decision counts
d = pd.read_csv(R + "q1_decision_counts.csv")
fig, ax = plt.subplots(figsize=(7, 4))
bars = ax.bar(d.decision_clean.str.title(), d.paper_count, color=COLORS[0], width=0.55)
ax.bar_label(bars, labels=[f"{v:,}" for v in d.paper_count], color=TEXT, padding=3)
ax.set_title("Q1. ICLR papers by decision")
ax.set_ylabel("Papers")
ax.grid(axis="x", visible=False)
save(fig, "q1_decision_counts.png")

# Q1.1: acceptance rate by year with z-score outliers
d = pd.read_csv(R + "q1_ext_acceptance_rate_zscore.csv")
fig, ax = plt.subplots(figsize=(8, 4.2))
ax.plot(d.year, d.acceptance_rate, color=COLORS[0], lw=2, marker="o", ms=6)
for _, r in d[d.year_type != "Normal Range"].iterrows():
    up = r.z_score > 0
    ax.annotate(f"{r.year_type}\nz = {r.z_score:+.2f}", (r.year, r.acceptance_rate),
                textcoords="offset points", xytext=(0, 10) if up else (-10, 12),
                ha="center" if up else "right", color=TEXT2, fontsize=9)
mean = d.acceptance_rate.mean()
ax.axhline(mean, color=TEXT2, lw=1, ls="--")
ax.text(d.year.min(), mean + 1.2, f"mean {mean:.1f}%", color=TEXT2, fontsize=9)
ax.set_xticks(d.year)
ax.set_ylim(-5, 60)
ax.set_ylabel("Acceptance rate (%)")
ax.set_title("Q1.1 Acceptance rate by year, with z-score outliers")
save(fig, "q1_ext_acceptance_rate_zscore.png")

# Q4.1: keyword growth
d = pd.read_csv(R + "q4_ext_keyword_growth_trajectory.csv")
fig, ax = plt.subplots(figsize=(8, 4.5))
for i, (k, g) in enumerate(d.groupby("keyword_clean", sort=False)):
    ax.plot(g.year, g.keyword_count, color=COLORS[i], lw=2, marker="o", ms=6,
            label=f"{k} ({int(g.keyword_count.iloc[-1])} in 2026)")
ax.set_yscale("log")
ax.set_xticks(range(2023, 2027))
ax.set_ylabel("Papers using keyword (log scale)")
ax.set_title("Q4.1 Growth of emerging keywords")
ax.legend(frameon=False, loc="upper left", fontsize=9)
save(fig, "q4_ext_keyword_growth.png")

# Q5.1: exact vs normalised title matches
d = pd.read_csv(R + "q5_ext_title_normalisation_matches.csv")
fig, ax = plt.subplots(figsize=(8, 4.2))
w, x = 0.38, range(len(d))
ax.bar([i - w / 2 - 0.01 for i in x], d.exact_title_matches, w, color=COLORS[0], label="Exact title match")
ax.bar([i + w / 2 + 0.01 for i in x], d.cleaned_title_matches, w, color=COLORS[1], label="Normalised title match")
ax.set_xticks(list(x))
ax.set_xticklabels(d.year)
ax.set_ylabel("ICLR papers matched in OpenAlex")
ax.set_title("Q5.1 OpenAlex matches: exact vs normalised titles")
ax.legend(frameon=False)
ax.grid(axis="x", visible=False)
save(fig, "q5_ext_title_normalisation.png")

# Q6.1: citations by score band
d = pd.read_csv(R + "q6_ext_score_band_citations.csv")
order = ["Low (below 4)", "Medium (4–6)", "High (6–8)", "Very High (8+)"]
d = d.set_index("score_band").loc[order].reset_index()
fig, ax = plt.subplots(figsize=(7.5, 4.2))
bars = ax.bar(d.score_band, d.avg_citations, color=COLORS[0], width=0.55)
ax.bar_label(bars, labels=[f"{v:.0f}  (n={n})" for v, n in zip(d.avg_citations, d.paper_count)],
             color=TEXT, padding=3, fontsize=9)
ax.set_ylabel("Average citations")
ax.set_xlabel("Mean reviewer score band")
ax.set_title("Q6.1 Citations by reviewer score band (Pearson r = 0.047)")
ax.grid(axis="x", visible=False)
save(fig, "q6_ext_score_band_citations.png")
