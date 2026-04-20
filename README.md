# 🏦 Bank Data Engineering Pipeline (Medallion Architecture)

## 📌 Layihə Haqqında
Bu layihə bank/maliyyə sektoruna aid məlumatların (Kredit portfeli, Mənfəət və Zərər hesabatı və s.) təmizlənməsi, çevrilməsi və analitik Dashboard-lar üçün hazır vəziyyətə gətirilməsini təmin edən tam avtomatlaşdırılmış ETL pipeline-dır.

## 🏗️ Arxitektura (Medallion Architecture)
Layihə qabaqcıl verilənlər mühəndisliyi prinsipləri əsasında 3 laydan ibarətdir:
- **🥉 Bronze Layer:** Məlumatların xam (raw) vəziyyətdə (CSV formatından) Obyekt-yönümlü Python kodu vasitəsilə SQL verilənlər bazasına yüklənməsi.
- **🥈 Silver Layer:** Məlumatların təmizlənməsi, `Forward-fill` metodu (Window Functions) ilə zaman boşluqlarının doldurulması və lazımsız sütunların silinməsi.
- **🥇 Gold Layer:** "Double-counting" xətalarının qarşısını alan `is_total_metric` məntiqinin tətbiqi və məlumatların BI alətləri (Power BI/Tableau) üçün optimallaşdırılmış "Long Format"-a çevrilməsi (`CROSS JOIN` Unpivot texnikası).

## 🛠️ İstifadə Olunan Texnologiyalar
- **Python:** OOP, Pandas, SQLAlchemy, Logging, Error Handling
- **SQL (MySQL):** Window Functions, CTE/Subqueries, Cross Joins, View yaradılması
- **Verilənlərin Modelləşdirilməsi:** Fact & Dimension tables, Unpivoting

## 🚀 Qovluq Strukturu
- `python_etl/`: CSV fayllarını oxuyub bazaya yazan OOP əsaslı Python skripti.
- `sql_transformations/`: Bronze -> Silver -> Gold laylarına keçidi təmin edən qabaqcıl SQL skripti.