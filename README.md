# NASDAQ Market Growth/Shrinkage Charting Dashboard

This is a production-grade React 18 + TypeScript web application for visualizing market structural growth and shrinkage trends of NASDAQ securities. The application is designed to ingest high-frequency data, process calculations on background worker threads, and render responsive, high-performance canvas visualizers.

---

## Key Features

1.  **Zero-Blocking Computations**: All mathematical calculations (Garman-Klass Volatility, Momentum Z-scores, Order Flow Imbalances, and Simple States) are processed off the main thread using an asynchronous Web Worker wrapped in **Comlink**.
2.  **High-Fidelity Rendering**: Standard animations are deactivated in **Chart.js 4.4** to keep canvas updates fast. Custom plugins draw vertical state transition markers and color-coded dominant regime bands.
3.  **Keyboard & Screen Reader Accessibility**:
    *   `ArrowLeft` and `ArrowRight` step through price bars.
    *   `Escape` closes detail overlays.
    *   `aria-live` region announces selected bar metrics automatically.
4.  **Drill Down Zoom Context**: Clicking on any bar highlights the point and slides up a detail panel showing a $\pm 30$-day context.

---

## Tech Stack

*   **Framework**: React 18 (Strict Mode)
*   **Tooling**: Vite 5
*   **State Management**: Zustand 4
*   **API Querying**: TanStack Query 5 (React Query)
*   **Calculations**: Web Worker + Comlink
*   **Data Deserialization**: Apache Arrow JS
*   **Canvas Charts**: Chart.js 4.4
*   **Styling**: Tailwind CSS 3.4

---

## Local Development Setup

### 1. Installation
Install project dependencies:
```bash
npm install
```

### 2. Run Development Server
Launches the local Vite server:
```bash
npm run dev
```

### 3. Run Unit Tests
Executes the test suite containing signal worker math benchmarks, Welford standard deviation updates, and DrillDownPanel DOM assertions:
```bash
npm run test
```

### 4. Launch Storybook
Opens the isolated Storybook environment to inspect components:
```bash
npm run storybook
```

### 5. Build for Production
Creates the optimized output bundle under the `dist/` directory, using manual Rollup chunk splitting to isolate core dependencies (react, chartjs, apache-arrow):
```bash
npm run build
```

---

## Architecture Design Reference

```
src/
├── app/                  # App entry point, TanStack client wrapper
├── features/
│   ├── chart/            # Primary bar chart visualizer
│   │   ├── components/   # MainChart, custom Chart.js plugins
│   │   ├── hooks/        # useSignals (orchestrates worker calls), useWebSocket
│   │   ├── store/        # Zustand useChartStore state
│   │   ├── types/        # TypeScript Interfaces
│   │   └── workers/      # signalWorker.ts (runs math inside Worker)
│   ├── controls/         # YearSelector, Sensitivity Thresholds
│   ├── metrics/          # KPI Stats (Growth frequency, avg score)
│   └── drill/            # DrillDownPanel detail side-panel
└── shared/
    ├── ui/               # Reusable Button, Card, and Slider components
    ├── utils/            # Arrow Stream Deserializer and Welford math
    └── api/              # Arrow Fetch & WebSocket stream clients
```
