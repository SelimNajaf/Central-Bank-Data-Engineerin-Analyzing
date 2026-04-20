-- ==============================================================================
-- MƏRHƏLƏ 1: SILVER LAYER (Məlumatların Təmizlənməsi və Zənginləşdirilməsi)
-- ==============================================================================

DROP TABLE IF EXISTS silver.area_structure;

-- Bronze layından məlumatları kopyalayırıq
CREATE TABLE silver.area_structure AS
SELECT * FROM bronze.area_structure;

-- Zaman ölçüləri üçün yeni sütunlar əlavə edirik
ALTER TABLE silver.area_structure
ADD COLUMN month INT,
ADD COLUMN year INT;

-- 'index' əsasında ili (year) irəli doğru doldururuq (Forward-fill)
UPDATE silver.area_structure t
JOIN (
    SELECT
        `index`,
        MAX(CASE WHEN `date` > 1000 THEN `date` END) OVER (ORDER BY `index` ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS filled_year
    FROM silver.area_structure
) AS x
ON t.`index` = x.`index`
SET t.year = x.filled_year;

-- Ayı (month) mənimsədirik və qeyri-ay sətirlərini (başlıqları) silirik
UPDATE silver.area_structure
SET month = `date`
WHERE `date` BETWEEN 1 AND 12;
        
DELETE FROM silver.area_structure
WHERE month IS NULL;

-- Lazımsız və arxitekturaya zərər verən sütunları silirik
ALTER TABLE silver.area_structure
DROP COLUMN `index`,
DROP COLUMN total_loans_share_pct,
DROP COLUMN trade_services_share_pct,
DROP COLUMN mining_energy_share_pct,
DROP COLUMN agriculture_share_pct,
DROP COLUMN construction_share_pct,
DROP COLUMN industry_manufacturing_share_pct,
DROP COLUMN transport_communication_share_pct,
DROP COLUMN households_share_pct,
DROP COLUMN real_estate_mortgage_share_pct,
DROP COLUMN state_entities_share_pct,
DROP COLUMN budget_orgs_share_pct,
DROP COLUMN other_sectors_share_pct,
DROP COLUMN letter_of_credit_share_pct,
DROP COLUMN guarantees_share_pct,
DROP COLUMN factoring_share_pct,
DROP COLUMN overdraft_share_pct;

-- ==============================================================================

DROP TABLE IF EXISTS silver.bank_sector_profit_loss;

CREATE TABLE silver.bank_sector_profit_loss AS
SELECT * FROM bronze.bank_sector_profit_loss;

UPDATE silver.bank_sector_profit_loss
SET date = STR_TO_DATE(date, '%d.%m.%y');

ALTER TABLE silver.bank_sector_profit_loss
ADD COLUMN year INT,
ADD COLUMN month INT;

UPDATE silver.bank_sector_profit_loss
SET year = YEAR(date), month = MONTH(date);

DELETE FROM silver.bank_sector_profit_loss
WHERE year NOT BETWEEN 2024 AND 2025;

ALTER TABLE silver.bank_sector_profit_loss
DROP COLUMN `index`,
DROP COLUMN `date`; 

-- ==============================================================================

DROP TABLE IF EXISTS silver.credit_structure;

CREATE TABLE silver.credit_structure AS
SELECT * FROM bronze.credit_structure;

-- Zaman sütunlarının əlavə edilməsi
ALTER TABLE silver.credit_structure
ADD COLUMN year INT,
ADD COLUMN month INT;

-- 'index' əsasında ili (year) doldurmaq (Forward-fill)
UPDATE silver.credit_structure t
JOIN (
    SELECT
        `index`,
        MAX(CASE WHEN `date` > 1000 THEN `date` END) OVER (ORDER BY `index` ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS filled_year
    FROM silver.credit_structure 
) AS x
ON t.`index` = x.`index`
SET t.year = x.filled_year;

-- Ayı (month) mənimsədirik
UPDATE silver.credit_structure
SET month = `date`
WHERE `date` BETWEEN 1 AND 12;

-- Dashboard-da hesablanmalı olan faizləri və texniki sütunları silirik
ALTER TABLE silver.credit_structure
DROP COLUMN `index`,
DROP COLUMN `date`,
DROP COLUMN state_owned_banks_share_pct,
DROP COLUMN private_banks_total_share_pct,
DROP COLUMN private_banks_foreign_capital_share_pct,
DROP COLUMN private_banks_100pct_foreign_share_pct,
DROP COLUMN non_bank_credit_institutions_share_pct;

-- ==============================================================================

-- TODO: Bu cədvəllərin təmizlənmə və Gold layına keçirilmə məntiqi növbəti mərhələdə əlavə olunacaq.

DROP TABLE IF EXISTS silver.depozit_ve_emtee_cemi;
CREATE TABLE silver.depozit_ve_emtee_cemi AS
SELECT * FROM bronze.depozit_ve_emtee_cemi;

DROP TABLE IF EXISTS silver.depozit_ve_emtee_kredit;
CREATE TABLE silver.depozit_ve_emtee_kredit AS
SELECT * FROM bronze.depozit_ve_emtee_kredit;

DROP TABLE IF EXISTS silver.dovlet_budcesi;
CREATE TABLE silver.dovlet_budcesi AS
SELECT * FROM bronze.dovlet_budcesi;

DROP TABLE IF EXISTS silver.effektiv_mezenne;
CREATE TABLE silver.effektiv_mezenne AS
SELECT * FROM bronze.effektiv_mezenne;

DROP TABLE IF EXISTS silver.fiziki_sexslerin_emtee_strukturu;
CREATE TABLE silver.fiziki_sexslerin_emtee_strukturu AS
SELECT * FROM bronze.fiziki_sexslerin_emtee_strukturu;

DROP TABLE IF EXISTS silver.macroiqtisadi_gostericiler;
CREATE TABLE silver.macroiqtisadi_gostericiler AS
SELECT * FROM bronze.macroiqtisadi_gostericiler;

DROP TABLE IF EXISTS silver.npl_structur;
CREATE TABLE silver.npl_structur AS
SELECT * FROM bronze.npl_structur;

DROP TABLE IF EXISTS silver.xarici_ticaret;
CREATE TABLE silver.xarici_ticaret AS
SELECT * FROM bronze.xarici_ticaret;


-- ==============================================================================
-- MƏRHƏLƏ 2: GOLD LAYER (Dashboard-a Hazır Long Format)
-- ==============================================================================
DROP TABLE IF EXISTS gold.loans_dashboard_long;

-- Alt Sorğu (Subquery) istifadə edərək amount-u formalaşdırır və NULL-ları süzürük
CREATE TABLE gold.loans_dashboard_long AS
SELECT * 
FROM (
    SELECT 
        t.year,
        t.month,
        c.category_type,
        c.category_name,
        CASE c.category_name
            -- 1. İqtisadi Sektorlar
            WHEN 'trade_services'          THEN t.trade_services_total
            WHEN 'mining_energy'           THEN t.mining_energy_total
            WHEN 'agriculture'             THEN t.agriculture_total
            WHEN 'construction'            THEN t.construction_total
            WHEN 'industry_manufacturing'  THEN t.industry_manufacturing_total
            WHEN 'transport_communication' THEN t.transport_communication_total
            WHEN 'households'              THEN t.households_total
            WHEN 'real_estate_mortgage'    THEN t.real_estate_mortgage_total
            WHEN 'state_entities'          THEN t.state_entities_total
            WHEN 'budget_orgs'             THEN t.budget_orgs_total
            WHEN 'other_sectors'           THEN t.other_sectors_total
            WHEN 'financial_sector_loans'  THEN t.financial_sector_loans_total
            
            -- 2. Bank / Maliyyə Məhsulları
            WHEN 'letter_of_credit'        THEN t.letter_of_credit_total
            WHEN 'guarantees'              THEN t.guarantees_total
            WHEN 'factoring'               THEN t.factoring_total
            WHEN 'overdraft'               THEN t.overdraft_total
            
            -- 3. Ümumi Portfel Metrikləri
            WHEN 'total_loans'             THEN t.total_loans_total
            WHEN 'total_overdue'           THEN t.total_loans_overdue
        END AS amount
    FROM silver.area_structure t
    CROSS JOIN (
        -- Virtual Kateqoriya Cədvəli
        SELECT 'Sector' AS category_type, 'trade_services' AS category_name UNION ALL
        SELECT 'Sector', 'mining_energy' UNION ALL
        SELECT 'Sector', 'agriculture' UNION ALL
        SELECT 'Sector', 'construction' UNION ALL
        SELECT 'Sector', 'industry_manufacturing' UNION ALL
        SELECT 'Sector', 'transport_communication' UNION ALL
        SELECT 'Sector', 'households' UNION ALL
        SELECT 'Sector', 'real_estate_mortgage' UNION ALL
        SELECT 'Sector', 'state_entities' UNION ALL
        SELECT 'Sector', 'budget_orgs' UNION ALL
        SELECT 'Sector', 'other_sectors' UNION ALL
        SELECT 'Sector', 'financial_sector_loans' UNION ALL
        
        SELECT 'Product', 'letter_of_credit' UNION ALL
        SELECT 'Product', 'guarantees' UNION ALL
        SELECT 'Product', 'factoring' UNION ALL
        SELECT 'Product', 'overdraft' UNION ALL
        
        SELECT 'Portfolio', 'total_loans' UNION ALL
        SELECT 'Portfolio', 'total_overdue'
    ) c
) final_data
WHERE amount IS NOT NULL;

-- SELECT * FROM gold.loans_dashboard_long LIMIT 100;

-- ==============================================================================

DROP TABLE IF EXISTS gold.pnl_dashboard_long;

CREATE TABLE gold.pnl_dashboard_long AS
SELECT * 
FROM (
    SELECT 
        t.year,
        t.month,
        c.category_type,
        c.category_name,
        c.is_total_metric,
        CASE c.category_name
            -- 1. Faiz Gəlirləri (Interest Income)
            WHEN 'interest_loans_total' THEN t.interest_loans_total
            WHEN 'less_special_provisions_interest' THEN t.less_special_provisions_interest
            WHEN 'interest_financial_sector' THEN t.interest_financial_sector
            WHEN 'interest_securities' THEN t.interest_securities
            WHEN 'interest_other' THEN t.interest_other
            WHEN 'interest_related_income' THEN t.interest_related_income  -- ALT CƏM
            
            -- 2. Faiz Xərcləri (Interest Expense)
            WHEN 'interest_deposits' THEN t.interest_deposits
            WHEN 'interest_time_deposits' THEN t.interest_time_deposits
            WHEN 'interest_financial_sector_borrowed' THEN t.interest_financial_sector_borrowed
            WHEN 'interest_other_expense' THEN t.interest_other_expense
            WHEN 'interest_expense' THEN t.interest_expense  -- ALT CƏM
            
            -- 3. Qeyri-Faiz Gəlirləri (Non-Interest Income)
            WHEN 'commission_account_services' THEN t.commission_account_services
            WHEN 'fx_income_loss' THEN t.fx_income_loss
            WHEN 'securities_sale_income_loss' THEN t.securities_sale_income_loss
            WHEN 'other_non_interest_income' THEN t.other_non_interest_income
            WHEN 'non_interest_income' THEN t.non_interest_income  -- ALT CƏM
            
            -- 4. Qeyri-Faiz Xərcləri (Non-Interest Expense)
            WHEN 'fixed_assets_costs' THEN t.fixed_assets_costs
            WHEN 'service_commission_costs' THEN t.service_commission_costs
            WHEN 'other_non_interest_expenses' THEN t.other_non_interest_expenses
            WHEN 'non_interest_expenses' THEN t.non_interest_expenses  -- ALT CƏM
            
            -- 5. Ehtiyatlar, Vergilər və Digər (Provisions & Taxes)
            WHEN 'loan_loss_provisions' THEN t.loan_loss_provisions
            WHEN 'other_income_expenses' THEN t.other_income_expenses
            WHEN 'profit_tax' THEN t.profit_tax
            
            -- 6. Əsas Gəlirlilik Göstəriciləri (Profitability Metrics) - HAMISI YEKUN
            WHEN 'net_interest_profit' THEN t.net_interest_profit
            WHEN 'operating_profit' THEN t.operating_profit
            WHEN 'profit_before_tax' THEN t.profit_before_tax
            WHEN 'net_profit' THEN t.net_profit
        END AS amount
    FROM silver.bank_sector_profit_loss t
    CROSS JOIN (
        -- Faiz Gəlirləri
        SELECT 'Interest Income' AS category_type, 'interest_loans_total' AS category_name, 0 AS is_total_metric UNION ALL
        SELECT 'Interest Income', 'less_special_provisions_interest', 0 UNION ALL
        SELECT 'Interest Income', 'interest_financial_sector', 0 UNION ALL
        SELECT 'Interest Income', 'interest_securities', 0 UNION ALL
        SELECT 'Interest Income', 'interest_other', 0 UNION ALL
        SELECT 'Interest Income', 'interest_related_income', 1 UNION ALL
        
        -- Faiz Xərcləri
        SELECT 'Interest Expense', 'interest_deposits', 0 UNION ALL
        SELECT 'Interest Expense', 'interest_time_deposits', 0 UNION ALL
        SELECT 'Interest Expense', 'interest_financial_sector_borrowed', 0 UNION ALL
        SELECT 'Interest Expense', 'interest_other_expense', 0 UNION ALL
        SELECT 'Interest Expense', 'interest_expense', 1 UNION ALL
        
        -- Qeyri-Faiz Gəlirləri
        SELECT 'Non-Interest Income', 'commission_account_services', 0 UNION ALL
        SELECT 'Non-Interest Income', 'fx_income_loss', 0 UNION ALL
        SELECT 'Non-Interest Income', 'securities_sale_income_loss', 0 UNION ALL
        SELECT 'Non-Interest Income', 'other_non_interest_income', 0 UNION ALL
        SELECT 'Non-Interest Income', 'non_interest_income', 1 UNION ALL
        
        -- Qeyri-Faiz Xərcləri
        SELECT 'Non-Interest Expense', 'fixed_assets_costs', 0 UNION ALL
        SELECT 'Non-Interest Expense', 'service_commission_costs', 0 UNION ALL
        SELECT 'Non-Interest Expense', 'other_non_interest_expenses', 0 UNION ALL
        SELECT 'Non-Interest Expense', 'non_interest_expenses', 1 UNION ALL
        
        -- Ehtiyatlar və Vergilər
        SELECT 'Provisions & Taxes', 'loan_loss_provisions', 0 UNION ALL
        SELECT 'Provisions & Taxes', 'other_income_expenses', 0 UNION ALL
        SELECT 'Provisions & Taxes', 'profit_tax', 0 UNION ALL
        
        -- Gəlirlilik Göstəriciləri (Hamısı yekun metriklərdir)
        SELECT 'Profitability', 'net_interest_profit', 1 UNION ALL
        SELECT 'Profitability', 'operating_profit', 1 UNION ALL
        SELECT 'Profitability', 'profit_before_tax', 1 UNION ALL
        SELECT 'Profitability', 'net_profit', 1
    ) c
) final_data
WHERE amount IS NOT NULL;

-- SELECT * FROM gold.pnl_dashboard_long;

-- ==============================================================================

DROP TABLE IF EXISTS gold.credit_dashboard_long;

CREATE TABLE gold.credit_dashboard_long AS
SELECT * 
FROM (
    SELECT 
        t.year,
        t.month,
        c.category_type,
        c.category_name,
        c.is_total_metric,
        CASE c.category_name
            -- 1. Yekun Portfel
            WHEN 'total_loans' THEN t.total_loans_mln_manat
            
            -- 2. Əsas İnstitut Tipləri
            WHEN 'state_owned_banks' THEN t.state_owned_banks_mln_manat
            WHEN 'private_banks_total' THEN t.private_banks_total_mln_manat
            WHEN 'non_bank_credit_institutions' THEN t.non_bank_credit_institutions_mln_manat
            
            -- 3. Özəl Bankların Alt Qrupları
            WHEN 'private_banks_foreign_capital' THEN t.private_banks_foreign_capital_mln_manat
            WHEN 'private_banks_100pct_foreign' THEN t.private_banks_100pct_foreign_mln_manat
        END AS amount_mln_manat
    FROM silver.credit_structure t
    CROSS JOIN (
        -- Kateqoriya Xəritəsi (Mapping)
        
        -- Yekun Portfel (Cəmləməyə daxil olmamalıdır)
        SELECT 'Portfolio Total' AS category_type, 'total_loans' AS category_name, 1 AS is_total_metric UNION ALL
        
        -- Əsas İnstitut Tipləri (Bunların 3-nün cəmi ümumi kreditləri verməlidir -> is_total_metric = 0)
        SELECT 'Institution Type', 'state_owned_banks', 0 UNION ALL
        SELECT 'Institution Type', 'private_banks_total', 0 UNION ALL
        SELECT 'Institution Type', 'non_bank_credit_institutions', 0 UNION ALL
        
        -- Özəl Bankların Detalları (Bunlar özəl bankların daxilindədir, ümumi cəmə yenidən qatılmamalıdır)
        SELECT 'Private Bank Subcategory', 'private_banks_foreign_capital', 1 UNION ALL
        SELECT 'Private Bank Subcategory', 'private_banks_100pct_foreign', 1
    ) c
) final_data
WHERE amount_mln_manat IS NOT NULL;

SELECT * FROM gold.credit_dashboard_long;

-- ==============================================================================

