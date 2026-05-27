import streamlit as st
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import pandas as pd
import numpy as np
import yfinance as yf

st.set_page_config(page_title="NASDAQ Growth Regime", layout="wide")
st.title("NASDAQ market growth and shrinkage tracker")

col1, col2, col3 = st.columns([2, 2, 2])
with col1:
    start = st.date_input("Start date", pd.Timestamp("2024-01-01"))
    end   = st.date_input("End date",   pd.Timestamp("2025-01-01"))
with col2:
    definition = st.selectbox("Growth definition",
        ["Simple (rising tide)", "Momentum z-score",
         "GK volatility regime", "OFI proxy", "Composite HMM"])
with col3:
    threshold = st.slider("Classification threshold", 0.1, 3.0, 1.0, 0.1)
    window    = st.slider("Lookback window (days)", 5, 60, 20)

ticker = st.text_input("Ticker symbol (e.g. ^IXIC for NASDAQ)", "^IXIC")

df = yf.download(ticker, start=start, end=end, interval="1d", progress=False)
if df.empty:
    st.error("No data found. Check your ticker symbol and date range.")
    st.stop()

df.columns = [c[0].lower() if isinstance(c, tuple) else c.lower()
              for c in df.columns]

W = window
df['ret_N']   = df['close'].pct_change(W) * 100
df['log_ret'] = np.log(df['close'] / df['close'].shift(1))
mu            = df['log_ret'].rolling(60).mean()
std           = df['log_ret'].rolling(60).std().replace(0, np.nan)
df['z_score'] = (df['log_ret'] - mu) / std

hi = df['high']
lo = df['low']
cl = df['close']
op = df['open']
gk_raw = 0.5 * (np.log(hi / lo)) ** 2 - (2 * np.log(2) - 1) * (np.log(cl / op)) ** 2
df['gk_vol']    = np.sqrt(gk_raw.clip(lower=0)) * 100
df['vol_ratio'] = df['gk_vol'].rolling(5).mean() / df['gk_vol'].rolling(20).mean()
df['vol_avg']   = df['volume'].rolling(W).mean()
df['vol_rel']   = (df['volume'] / df['vol_avg'] - 1) * 100
signed          = np.sign(df['close'].diff()) * df['volume'] / df['vol_avg'].replace(0, np.nan)
df['ofi']       = signed.ewm(alpha=0.06, adjust=False).mean()
df['composite'] = (0.40 * df['z_score']
                 + 0.25 * (df['vol_ratio'] - 1) * 3
                 + 0.35 * df['ofi'] * 1.2)

STATE_COLORS = {
    'Strong growth':  '#3B6D11',
    'Weak growth':    '#97C459',
    'Flat':           '#888780',
    'Weak decline':   '#F09595',
    'Strong decline': '#A32D2D',
}

T = threshold

def classify(s):
    if pd.isna(s):   return 'Flat'
    if s >  T * 1.5: return 'Strong growth'
    if s >  T * 0.5: return 'Weak growth'
    if s < -T * 1.5: return 'Strong decline'
    if s < -T * 0.5: return 'Weak decline'
    return 'Flat'

df['state'] = df['composite'].apply(classify)
df = df.dropna(subset=['composite'])

if df.empty:
    st.warning("Not enough data to compute signals. Try a longer date range.")
    st.stop()

fig = make_subplots(
    rows=3, cols=1,
    row_heights=[0.55, 0.25, 0.20],
    shared_xaxes=True,
    vertical_spacing=0.05,
    subplot_titles=(
        'Composite growth signal',
        'Close price',
        'Volume vs 20-day average'
    )
)

for state, color in STATE_COLORS.items():
    mask = df['state'] == state
    if mask.sum() == 0:
        continue
    fig.add_trace(go.Bar(
        x=df[mask].index,
        y=df[mask]['composite'],
        name=state,
        marker_color=color,
        marker_line_width=0,
        hovertemplate=(
            '<b>%{x|%Y-%m-%d}</b><br>'
            + 'State: ' + state + '<br>'
            + 'Score: %{y:.2f}<extra></extra>'
        )
    ), row=1, col=1)

fig.add_trace(go.Scatter(
    x=df.index,
    y=df['close'],
    name='Close price',
    line=dict(color='#639922', width=1.5),
    fill='tozeroy',
    fillcolor='rgba(99,153,34,0.08)',
    hovertemplate='%{x|%Y-%m-%d}<br>Close: %{y:,.0f}<extra></extra>'
), row=2, col=1)

vol_colors = ['#378ADD' if v > 0 else '#B5D4F4'
              for v in df['vol_rel'].fillna(0)]
fig.add_trace(go.Bar(
    x=df.index,
    y=df['vol_rel'],
    name='Vol vs avg',
    marker_color=vol_colors,
    marker_line_width=0,
    hovertemplate='%{x|%Y-%m-%d}<br>Vol vs avg: %{y:.1f}%<extra></extra>'
), row=3, col=1)

fig.update_layout(
    height=700,
    barmode='overlay',
    showlegend=True,
    legend=dict(orientation='h', y=1.08, x=0),
    hovermode='x unified',
    plot_bgcolor='white',
    paper_bgcolor='white',
    margin=dict(l=50, r=30, t=80, b=40),
    font=dict(family='Arial, sans-serif', size=12)
)
fig.update_yaxes(row=1, col=1,
    gridcolor='rgba(128,128,128,0.1)',
    tickformat='.1f')
fig.update_yaxes(row=2, col=1,
    gridcolor='rgba(128,128,128,0.1)',
    tickformat=',.0f')
fig.update_yaxes(row=3, col=1,
    gridcolor='rgba(128,128,128,0.1)',
    ticksuffix='%')
fig.update_xaxes(showgrid=False)

st.plotly_chart(fig, use_container_width=True)

st.divider()
st.subheader("Summary statistics")

c1, c2, c3, c4 = st.columns(4)
with c1:
    st.metric("Current state", df['state'].iloc[-1])
with c2:
    grow = df['state'].isin(['Strong growth', 'Weak growth']).sum()
    pct  = grow / len(df) * 100
    st.metric("Growth days", f"{grow} ({pct:.0f}%)")
with c3:
    st.metric("Avg return", f"{df['ret_N'].mean():.1f}%")
with c4:
    st.metric("Best score", f"+{df['composite'].max():.2f}")

st.divider()
csv = df[['close', 'volume', 'z_score', 'composite', 'state']].to_csv()
st.download_button(
    label="Download features as CSV",
    data=csv,
    file_name="market_features.csv",
    mime="text/csv"
)
