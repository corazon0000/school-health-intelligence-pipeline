"""
dashboard.py  —  Script 3: School Health Dashboard
Jalankan: streamlit run dashboard.py
"""

import json
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

# ============================================================
# CONFIG
# ============================================================
MART_CSV    = "out/mart_school_health.csv"
ALERTS_CSV  = "out/alerts.csv"
DIGEST_JSON = "out/alerts_digest.json"

st.set_page_config(
    page_title="School Health — SmartSchool",
    page_icon="🏫",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ============================================================
# CSS — dark, bersih, font yang terbaca
# ============================================================
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap');

html, body, [class*="css"] {
    font-family: 'Inter', sans-serif;
    background-color: #0d0d14;
    color: #e0e0f0;
}
.stApp { background: #0d0d14; }

/* Sidebar */
[data-testid="stSidebar"] {
    background: #11111c !important;
    border-right: 1px solid #1f1f35;
}

/* Metric cards — angka tidak terpotong */
[data-testid="stMetric"] {
    background: #16162a;
    border: 1px solid #2a2a48;
    border-radius: 14px;
    padding: 20px !important;
}
[data-testid="stMetricLabel"] p {
    font-size: 0.78rem !important;
    font-weight: 600 !important;
    letter-spacing: 0.06em !important;
    text-transform: uppercase !important;
    color: #7070a0 !important;
}
[data-testid="stMetricValue"] {
    font-size: 2.4rem !important;
    font-weight: 700 !important;
    color: #ffffff !important;
    white-space: nowrap !important;
    overflow: visible !important;
}
[data-testid="stMetricDelta"] {
    font-size: 0.78rem !important;
    color: #7070a0 !important;
}

/* Headers */
h1, h2, h3 {
    font-family: 'Inter', sans-serif !important;
    color: #e0e0f0 !important;
}

/* Section label */
.sec {
    font-size: 0.7rem;
    font-weight: 700;
    letter-spacing: 0.12em;
    text-transform: uppercase;
    color: #404068;
    margin: 24px 0 10px;
}

/* Tabs */
[data-testid="stTabs"] button {
    font-size: 0.85rem;
    font-weight: 600;
    color: #5050a0;
}
[data-testid="stTabs"] button[aria-selected="true"] {
    color: #e0e0f0 !important;
    border-bottom: 2px solid #6060ff !important;
}

/* Alert card */
.acard {
    background: #16162a;
    border-radius: 12px;
    padding: 14px 18px;
    margin-bottom: 8px;
    border-left: 4px solid #6060ff;
}
.acard.cr { border-left-color: #ff3355; }
.acard.hi { border-left-color: #ff7733; }
.acard.me { border-left-color: #ffbb33; }
.acard-name { font-weight: 700; font-size: 0.95rem; color: #e0e0f0; }
.acard-meta { font-size: 0.77rem; color: #6060a0; margin-top: 3px; }
.acard-msg  { font-size: 0.83rem; color: #a0a0cc; margin-top: 6px; }

/* Scrollbar */
::-webkit-scrollbar { width: 4px; }
::-webkit-scrollbar-thumb { background: #2a2a48; border-radius: 4px; }
</style>
""", unsafe_allow_html=True)

# Plotly theme
PT = dict(
    paper_bgcolor="#0d0d14",
    plot_bgcolor="#13131f",
    font=dict(family="Inter", color="#9090b0", size=12),
    margin=dict(t=36, b=16, l=16, r=16),
)

# ============================================================
# LOAD DATA
# ============================================================
@st.cache_data(ttl=300)
def load():
    m = pd.read_csv(MART_CSV)
    a = pd.read_csv(ALERTS_CSV)
    with open(DIGEST_JSON) as f:
        d = json.load(f)
    return m, a, d

try:
    mart, alerts, digest = load()
except FileNotFoundError:
    st.error("File tidak ditemukan. Jalankan Script 1 dan Script 2 dulu.")
    st.stop()

# ============================================================
# SIDEBAR
# ============================================================
with st.sidebar:
    st.markdown("### 🏫 School Health")
    st.caption(f"Data: {digest['generated_at'][:10]}")
    st.divider()

    status_f = st.multiselect("Status Kontrak", ["aktif", "expired"],
                               default=["aktif", "expired"])
    degree_f = st.multiselect("Jenjang", sorted(mart["degree"].unique()),
                               default=sorted(mart["degree"].unique()))
    type_f   = st.multiselect("Tipe Bisnis", ["B2B", "B2G"], default=["B2B", "B2G"])
    tier_f   = st.multiselect("Engagement", ["High", "Medium", "Low", "Ghost"],
                               default=["High", "Medium", "Low", "Ghost"])
    st.divider()
    st.caption("**Total Sekolah** " + str(f"{digest['summary']['total_schools']:,}"))
    st.caption("**Real Users** " + str(f"{digest['summary']['total_real_users']:,}"))

# Filter
df = mart[
    mart["contract_status"].isin(status_f) &
    mart["degree"].isin(degree_f) &
    mart["school_type"].isin(type_f) &
    mart["engagement_tier"].isin(tier_f)
].copy()

# ============================================================
# HEADER
# ============================================================
st.markdown("## 🏫 School Health Intelligence")
st.caption("SmartSchool · Portfolio & Engagement Monitor")
st.markdown("<div style='height:8px'></div>", unsafe_allow_html=True)

# ============================================================
# ROW 1 METRICS — 3 kolom supaya angka tidak kepotong
# ============================================================
r1c1, r1c2, r1c3 = st.columns(3)
r1c1.metric("Sekolah Aktif",   f"{int((df['contract_status']=='aktif').sum()):,}")
r1c2.metric("Sekolah Expired", f"{int((df['contract_status']=='expired').sum()):,}")
r1c3.metric("Ghost Schools",   f"{int(df['is_ghost_school'].sum()):,}",
            help="Sekolah aktif tapi tidak ada aktivitas siswa sama sekali")

st.markdown("<div style='height:6px'></div>", unsafe_allow_html=True)

r2c1, r2c2, r2c3 = st.columns(3)
r2c1.metric("High Engagement",  f"{int((df['engagement_tier']=='High').sum()):,}",
            help="Sekolah yang siswanya beneran belajar di platform")
r2c2.metric("Expire ≤ 30 Hari", f"{int((df['days_to_expire'].between(0,30)).sum()):,}",
            help="Perlu follow-up renewal sekarang")
r2c3.metric("Pernah Login", f"{int(df['users_ever_login'].fillna(0).sum()):,}",
            help="User yang benar-benar pernah login ke platform (bukan akun placeholder)")

st.markdown("<div style='height:20px'></div>", unsafe_allow_html=True)

# ============================================================
# TABS
# ============================================================
tab1, tab2, tab3, tab4, tab5 = st.tabs([
    "📊 Overview",
    "🔥 Engagement",
    "⚠️ Alerts",
    "📈 Growth",
    "🔍 Cari Sekolah",
])

# ──────────────────────────────────────────────────────
# TAB 1 — OVERVIEW
# ──────────────────────────────────────────────────────
with tab1:
    col1, col2 = st.columns(2, gap="large")

    with col1:
        # Donut aktif vs expired
        st.markdown("<div class='sec'>Status Kontrak</div>", unsafe_allow_html=True)
        sc = df["contract_status"].value_counts()
        fig = go.Figure(go.Pie(
            labels=sc.index, values=sc.values, hole=0.62,
            marker=dict(colors=["#44cc77", "#ff3355"],
                        line=dict(color="#0d0d14", width=3)),
            textinfo="label+percent",
            textfont=dict(size=13, family="Inter"),
        ))
        fig.update_layout(**PT, height=300, showlegend=False,
            annotations=[dict(
                text=f"<b>{len(df):,}</b><br>sekolah",
                x=0.5, y=0.5, showarrow=False,
                font=dict(size=16, color="#e0e0f0", family="Inter"),
            )])
        st.plotly_chart(fig, use_container_width=True)

        # B2B vs B2G
        st.markdown("<div class='sec'>B2B vs B2G per Status</div>", unsafe_allow_html=True)
        b2 = df.groupby(["school_type", "contract_status"]).size().reset_index(name="n")
        fig2 = px.bar(b2, x="school_type", y="n", color="contract_status",
                      color_discrete_map={"aktif": "#44cc77", "expired": "#ff3355"},
                      barmode="group", text="n")
        fig2.update_traces(textposition="outside",
                           textfont=dict(size=12, color="#e0e0f0"),
                           marker_line_width=0)
        fig2.update_layout(**PT, height=280,
                           xaxis=dict(title="", gridcolor="#1f1f35"),
                           yaxis=dict(title="", gridcolor="#1f1f35"),
                           legend=dict(orientation="h", y=1.1, title=""))
        st.plotly_chart(fig2, use_container_width=True)

    with col2:
        # Per jenjang
        st.markdown("<div class='sec'>Sekolah per Jenjang</div>", unsafe_allow_html=True)
        deg = df.groupby(["degree", "contract_status"]).size().reset_index(name="n")
        fig3 = px.bar(deg, x="n", y="degree", color="contract_status",
                      color_discrete_map={"aktif": "#6060ff", "expired": "#ff3355"},
                      barmode="stack", orientation="h",
                      category_orders={"degree": ["SD","SMP","SMA/SMK","Lainnya"]},
                      text="n")
        fig3.update_traces(textposition="inside",
                           textfont=dict(size=11, color="#ffffff"),
                           marker_line_width=0)
        fig3.update_layout(**PT, height=280,
                           xaxis=dict(title="", gridcolor="#1f1f35"),
                           yaxis=dict(title="", gridcolor="#1f1f35"),
                           legend=dict(orientation="h", y=1.1, title=""))
        st.plotly_chart(fig3, use_container_width=True)

        # Churn risk
        st.markdown("<div class='sec'>Churn Risk</div>", unsafe_allow_html=True)
        ro = ["Critical", "High", "Medium", "Low"]
        rc = {"Critical":"#ff3355","High":"#ff7733","Medium":"#ffbb33","Low":"#44cc77"}
        rd = df["churn_risk"].value_counts().reindex(ro, fill_value=0)
        fig4 = go.Figure(go.Bar(
            x=rd.index, y=rd.values,
            marker_color=[rc[r] for r in rd.index],
            text=rd.values, textposition="outside",
            textfont=dict(size=13, color="#e0e0f0"),
            marker_line_width=0,
        ))
        fig4.update_layout(**PT, height=280,
                           xaxis=dict(title="", gridcolor="#1f1f35"),
                           yaxis=dict(title="", gridcolor="#1f1f35"))
        st.plotly_chart(fig4, use_container_width=True)

# ──────────────────────────────────────────────────────
# TAB 2 — ENGAGEMENT
# ──────────────────────────────────────────────────────
with tab2:
    col1, col2 = st.columns(2, gap="large")

    with col1:
        # Engagement tier donut
        st.markdown("<div class='sec'>Engagement Tier</div>", unsafe_allow_html=True)
        tc = {"High":"#44aaff","Medium":"#44cc77","Low":"#ffbb33","Ghost":"#303050"}
        td = df["engagement_tier"].value_counts()
        fig5 = go.Figure(go.Pie(
            labels=td.index, values=td.values, hole=0.55,
            marker=dict(colors=[tc.get(t,"#6060ff") for t in td.index],
                        line=dict(color="#0d0d14", width=3)),
            textinfo="label+value",
            textfont=dict(size=12),
        ))
        fig5.update_layout(**PT, height=320, showlegend=False)
        st.plotly_chart(fig5, use_container_width=True)

        st.info(f"""
**Apa artinya?**
- 🔵 **High** ({int((df['engagement_tier']=='High').sum())} sekolah) — ada aktivitas belajar nyata
- 🟢 **Medium** ({int((df['engagement_tier']=='Medium').sum())} sekolah) — ada user, ada login
- 🟡 **Low** ({int((df['engagement_tier']=='Low').sum())} sekolah) — ada user, jarang aktif
- ⚫ **Ghost** ({int((df['engagement_tier']=='Ghost').sum())} sekolah) — bayar kontrak, tidak dipakai
""")

    with col2:
        # Top 15 study hours
        st.markdown("<div class='sec'>Top 15 Sekolah — Jam Belajar</div>",
                    unsafe_allow_html=True)
        top = (df[df["study_hours"] > 0]
               .nlargest(15, "study_hours")
               [["school_name","degree","study_hours","total_real_users"]]
               .sort_values("study_hours"))
        top["label"] = top["school_name"].str[:30]
        fig6 = px.bar(top, x="study_hours", y="label",
                      color="study_hours",
                      color_continuous_scale=["#1a1a40","#6060ff","#44aaff"],
                      orientation="h",
                      hover_data={"total_real_users": True, "degree": True})
        fig6.update_layout(**PT, height=460,
                           coloraxis_showscale=False,
                           xaxis=dict(title="Jam Belajar", gridcolor="#1f1f35"),
                           yaxis=dict(title="", gridcolor="#1f1f35"))
        fig6.update_traces(marker_line_width=0)
        st.plotly_chart(fig6, use_container_width=True)

# ──────────────────────────────────────────────────────
# TAB 3 — ALERTS
# ──────────────────────────────────────────────────────
with tab3:
    # Summary
    ac1, ac2, ac3 = st.columns(3)
    ac1.metric("🔴 Critical", digest["alerts_count"]["CRITICAL"],
               help="Sekolah expired yang pernah punya package — belum perpanjang")
    ac2.metric("🟠 High",     digest["alerts_count"]["HIGH"],
               help="Kontrak habis dalam 30 hari")
    ac3.metric("🟡 Medium",   digest["alerts_count"]["MEDIUM"],
               help="Ghost school atau expire dalam 90 hari")

    st.markdown("<div style='height:12px'></div>", unsafe_allow_html=True)

    # Filter
    fc1, fc2 = st.columns(2)
    with fc1:
        sev_f = st.multiselect("Filter Severity",
                               ["CRITICAL", "HIGH", "MEDIUM"],
                               default=["HIGH", "CRITICAL"])
    with fc2:
        atype_f = st.multiselect("Filter Tipe Alert",
                                 alerts["alert_type"].unique().tolist(),
                                 default=alerts["alert_type"].unique().tolist())

    shown = alerts[
        alerts["severity"].isin(sev_f) &
        alerts["alert_type"].isin(atype_f)
    ].drop_duplicates(subset=["school_id", "alert_type"])

    st.caption(f"Menampilkan {min(len(shown), 100):,} dari {len(shown):,} alerts")
    st.markdown("<div style='height:8px'></div>", unsafe_allow_html=True)

    sev_cls  = {"CRITICAL": "cr", "HIGH": "hi", "MEDIUM": "me"}
    sev_icon = {"CRITICAL": "🔴", "HIGH": "🟠", "MEDIUM": "🟡"}
    type_label = {
        "RENEWAL_URGENT":  "⏰ Renewal Urgent",
        "RENEWAL_WARNING": "🔔 Renewal Warning",
        "GHOST_SCHOOL":    "👻 Ghost School",
        "CHURN_CRITICAL":  "💀 Churn Critical",
    }

    for _, row in shown.head(100).iterrows():
        cls  = sev_cls.get(row["severity"], "me")
        icon = sev_icon.get(row["severity"], "⚪")
        users = int(row["total_real_users"]) if pd.notna(row["total_real_users"]) else 0
        dte   = row["days_to_expire"]
        if pd.notna(dte):
            dte_str = f"Expire {int(dte)} hari lagi" if dte >= 0 else f"Expired {abs(int(dte))} hari lalu"
        else:
            dte_str = "—"

        tlabel = type_label.get(row["alert_type"], row["alert_type"])
        st.markdown(f"""
        <div class="acard {cls}">
            <div class="acard-name">{icon} {row['school_name']}</div>
            <div class="acard-meta">{tlabel} &nbsp;·&nbsp; {row['degree']} &nbsp;·&nbsp;
            {row['school_type']} &nbsp;·&nbsp; {users} users &nbsp;·&nbsp; {dte_str}</div>
            <div class="acard-msg">{row['alert_message']}</div>
        </div>
        """, unsafe_allow_html=True)

    if len(shown) > 100:
        st.info("Gunakan filter severity untuk mempersempit hasil.")

# ──────────────────────────────────────────────────────
# TAB 4 — GROWTH
# ──────────────────────────────────────────────────────
with tab4:
    col1, col2 = st.columns(2, gap="large")

    with col1:
        # Akuisisi per tahun
        st.markdown("<div class='sec'>Akuisisi Sekolah per Tahun</div>",
                    unsafe_allow_html=True)
        cohort = (df[df["acquisition_year"] >= 2021]
                  .groupby("acquisition_year").size().reset_index(name="n"))
        fig7 = px.bar(cohort, x="acquisition_year", y="n",
                      color_discrete_sequence=["#6060ff"], text="n")
        fig7.update_traces(textposition="outside",
                           textfont=dict(size=14, color="#e0e0f0"),
                           marker_line_width=0)
        fig7.update_layout(**PT, height=320,
                           xaxis=dict(title="", gridcolor="#1f1f35", dtick=1),
                           yaxis=dict(title="Jumlah Sekolah", gridcolor="#1f1f35"))
        st.plotly_chart(fig7, use_container_width=True)

        # Renewal pipeline
        st.markdown("<div class='sec'>Sekolah yang Segera Expire</div>",
                    unsafe_allow_html=True)
        pp = digest["renewal_pipeline"]
        pf = pd.DataFrame({
            "Window": ["≤ 30 hari", "≤ 60 hari", "≤ 90 hari"],
            "Sekolah": [pp["expire_30_days"], pp["expire_60_days"], pp["expire_90_days"]],
        })
        fig8 = px.bar(pf, x="Window", y="Sekolah",
                      color_discrete_sequence=["#ff7733"], text="Sekolah")
        fig8.update_traces(textposition="outside",
                           textfont=dict(size=14, color="#e0e0f0"),
                           marker_line_width=0)
        fig8.update_layout(**PT, height=280,
                           xaxis=dict(title="", gridcolor="#1f1f35"),
                           yaxis=dict(title="", gridcolor="#1f1f35"))
        st.plotly_chart(fig8, use_container_width=True)

    with col2:
        # Churn rate B2B vs B2G
        st.markdown("<div class='sec'>Churn Rate — B2B vs B2G</div>",
                    unsafe_allow_html=True)
        cr = (df.groupby("school_type")
              .apply(lambda g: round(100 * (g["contract_status"]=="expired").sum() / len(g), 1),
                     include_groups=False)
              .reset_index(name="churn_pct"))
        fig9 = px.bar(cr, x="school_type", y="churn_pct",
                      color="school_type",
                      color_discrete_map={"B2B": "#6060ff", "B2G": "#ff3355"},
                      text="churn_pct")
        fig9.update_traces(texttemplate="%{text}%", textposition="outside",
                           textfont=dict(size=16, color="#e0e0f0"),
                           marker_line_width=0)
        fig9.update_layout(**PT, height=300, showlegend=False,
                           xaxis=dict(title="", gridcolor="#1f1f35"),
                           yaxis=dict(title="Churn Rate (%)", gridcolor="#1f1f35",
                                      range=[0, 100]))
        st.plotly_chart(fig9, use_container_width=True)

        # Akuisisi B2B vs B2G per tahun
        st.markdown("<div class='sec'>Akuisisi B2B vs B2G per Tahun</div>",
                    unsafe_allow_html=True)
        ct = (df[df["acquisition_year"] >= 2021]
              .groupby(["acquisition_year","school_type"])
              .size().reset_index(name="n"))
        fig10 = px.line(ct, x="acquisition_year", y="n", color="school_type",
                        color_discrete_map={"B2B":"#6060ff","B2G":"#44cc77"},
                        markers=True, text="n")
        fig10.update_traces(line=dict(width=2.5),
                            marker=dict(size=9),
                            textposition="top center",
                            textfont=dict(size=11, color="#e0e0f0"))
        fig10.update_layout(**PT, height=300,
                            xaxis=dict(title="", gridcolor="#1f1f35", dtick=1),
                            yaxis=dict(title="", gridcolor="#1f1f35"),
                            legend=dict(orientation="h", y=1.1, title=""))
        st.plotly_chart(fig10, use_container_width=True)

        # Insight box
        st.info("""
**Insight utama:**
- Growth akuisisi **3× setiap tahun** (2023→2024→2025)
- **B2G churn sangat tinggi** — program sekolah negeri mayoritas tidak perpanjang
- **B2B lebih sehat** — churn jauh lebih rendah
""")

# ──────────────────────────────────────────────────────
# TAB 5 — CARI SEKOLAH
# ──────────────────────────────────────────────────────
with tab5:
    search = st.text_input("🔍 Cari nama sekolah", placeholder="contoh: SDN Tejakula")

    cols_show = [
        "school_name", "degree", "school_type", "contract_status",
        "days_to_expire", "total_real_users", "study_hours",
        "engagement_tier", "churn_risk", "acquisition_year",
    ]

    result = df[cols_show].copy()
    if search:
        result = result[df["school_name"].str.contains(search, case=False, na=False)]

    st.caption(f"{len(result):,} sekolah ditemukan")

    st.dataframe(
        result,
        use_container_width=True,
        height=500,
        column_config={
            "school_name":      st.column_config.TextColumn("Nama Sekolah", width="large"),
            "degree":           st.column_config.TextColumn("Jenjang"),
            "school_type":      st.column_config.TextColumn("Tipe"),
            "contract_status":  st.column_config.TextColumn("Status"),
            "days_to_expire":   st.column_config.NumberColumn("Sisa Hari", format="%d hr"),
            "users_ever_login":  st.column_config.NumberColumn("Pernah Login"),
            "total_real_users": st.column_config.NumberColumn("Akun Terdaftar"),
            "study_hours":      st.column_config.NumberColumn("Jam Belajar", format="%.1f"),
            "engagement_tier":  st.column_config.TextColumn("Engagement"),
            "churn_risk":       st.column_config.TextColumn("Churn Risk"),
            "acquisition_year": st.column_config.NumberColumn("Tahun Bergabung"),
        },
    )

    csv = result.to_csv(index=False, encoding="utf-8-sig")
    st.download_button("⬇️ Download CSV", csv,
                       "school_health.csv", "text/csv")