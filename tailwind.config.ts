import type { Config } from 'tailwindcss';

export default {
  content: [
    "./index.html",
    "./src/**/*.{js,ts,jsx,tsx}",
  ],
  theme: {
    extend: {
      colors: {
        state: {
          'strong-growth': '#3B6D11',
          'weak-growth': '#97C459',
          'flat': '#888780',
          'weak-decline': '#F09595',
          'strong-decline': '#A32D2D',
        },
        bg: {
          light: '#ffffff',
          dark: '#0b0b0c',
        },
      },
      fontFamily: {
        sans: [
          'system-ui',
          '-apple-system',
          'BlinkMacSystemFont',
          '"Segoe UI"',
          'Roboto',
          'sans-serif'
        ],
        mono: [
          'ui-monospace',
          'SFMono-Regular',
          'Menlo',
          'Monaco',
          'Consolas',
          'monospace'
        ],
      },
      borderRadius: {
        'control': '8px',
        'card': '12px',
      },
      borderWidth: {
        'card': '0.5px',
      },
    },
  },
  plugins: [],
} satisfies Config;
