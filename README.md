# School Health Intelligence Pipeline 🏫📊

[![Python](https://img.shields.io/badge/Python-3.9+-yellow?logo=python)](https://www.python.org/)
[![Streamlit](https://img.shields.io/badge/Streamlit-Dashboard-FF4B4B?logo=streamlit)](https://streamlit.io/)
[![DuckDB](https://img.shields.io/badge/Database-DuckDB-FFF000?logo=duckdb)](https://duckdb.org/)

School Health Intelligence adalah sebuah platform **Business Intelligence (BI)** dan **Data Pipeline** end-to-end yang dirancang untuk memantau "kesehatan" ekosistem sekolah pada platform SmartSchool secara otomatis.

## 🚀 Gambaran Umum
Sistem ini mengautomasi proses identifikasi risiko *churn*, analisis keterlibatan pengguna (*engagement tiering*), dan sistem peringatan dini melalui bot Telegram. Data ditarik dari PostgreSQL produksi, diolah menjadi Data Mart, dan disajikan melalui dashboard interaktif.

## 🏗️ Arsitektur Sistem
Pipeline data ini mengikuti alur kerja yang terintegrasi:
1.  **Extract**: Migrasi data master sekolah dan log aktivitas dari PostgreSQL ke format **Parquet**.
2.  **Transform**: Pembersihan data *dummy*, standarisasi format, dan perhitungan metrik bisnis (Engagement & Churn Risk) menggunakan **DuckDB**.
3.  **Notify**: Pengiriman ringkasan harian (*Daily Digest*) ke grup Telegram melalui Bot API.
4.  **Visualize**: Dashboard **Streamlit** dengan visualisasi interaktif menggunakan Plotly.

## 📁 Struktur Repositori
```text
├── pipeline/
│   ├── extract_to_ducklake.py   # ETL: Ekstraksi data ke Data Lake
│   └── transform.py             # Business Logic & Data Mart Generator
├── run_pipeline.py              # Main Orchestrator & Telegram Notifier
├── dashboard.py                 # Streamlit UI Code
├── requirements.txt             # Dependensi Library
└── README.md                    # Dokumentasi Proyek
