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

-- SELECT * FROM gold.credit_dashboard_long;

-- ==============================================================================

DROP TABLE IF EXISTS gold.deposits_dashboard_long;

CREATE TABLE gold.deposits_dashboard_long AS
SELECT * 
FROM (
    SELECT 
        t.year,
        t.month,
        c.category_type,
        c.category_name,
        c.currency,          -- YENİ ÖLÇÜ: Valyuta filtri üçün (AZN / FX)
        c.term_type,         -- YENİ ÖLÇÜ: Müddət filtri üçün (Demand / Time)
        c.is_total_metric,   -- 1 = Yekun, 0 = Toplanıla bilən alt komponentlər
        CASE c.category_name
            WHEN 'total_deposits' THEN t.total_deposits
            WHEN 'deposits_manat_demand' THEN t.deposits_manat_demand
            WHEN 'deposits_manat_time' THEN t.deposits_manat_time
            WHEN 'deposits_fx_demand' THEN t.deposits_fx_demand
            WHEN 'deposits_fx_time' THEN t.deposits_fx_time
        END AS amount
    FROM silver.depozit_ve_emtee_cemi t
    CROSS JOIN (
        -- Yekun Portfel
        SELECT 'Portfolio Total' AS category_type, 'total_deposits' AS category_name, 'All' AS currency, 'All' AS term_type, 1 AS is_total_metric UNION ALL
        
        -- Alt Komponentlər (Buradakı 4 rəqəmin cəmi total_deposits-i verir)
        SELECT 'Deposit Component', 'deposits_manat_demand', 'AZN', 'Demand', 0 UNION ALL
        SELECT 'Deposit Component', 'deposits_manat_time',   'AZN', 'Time',   0 UNION ALL
        SELECT 'Deposit Component', 'deposits_fx_demand',    'FX',  'Demand', 0 UNION ALL
        SELECT 'Deposit Component', 'deposits_fx_time',      'FX',  'Time',   0
    ) c
) final_data
WHERE amount IS NOT NULL;

-- SELECT * FROM gold.deposits_dashboard_long LIMIT 100;

-- ==============================================================================

DROP TABLE IF EXISTS gold.deposits_sector_dashboard_long;

CREATE TABLE gold.deposits_sector_dashboard_long AS
SELECT * 
FROM (
    SELECT 
        t.year,
        t.month,
        c.category_type,
        c.category_name,
        c.sector,
        c.currency,
        c.term_type,  
        c.is_total_metric,
        CASE c.category_name
            -- Yekun Portfel
            WHEN 'total_deposits' THEN t.total_deposits
            
            -- Sektor Yekunları
            WHEN 'households_total' THEN t.households_total
            WHEN 'financial_corporations_total' THEN t.financial_corporations_total
            WHEN 'non_financial_corporations_total' THEN t.non_financial_corporations_total
            
            -- Əhali (Households) Detalları
            WHEN 'households_manat_demand' THEN t.households_manat_demand
            WHEN 'households_manat_time' THEN t.households_manat_time
            WHEN 'households_fx_demand' THEN t.households_fx_demand
            WHEN 'households_fx_time' THEN t.households_fx_time
            
            -- Maliyyə Təşkilatları Detalları
            WHEN 'financial_corporations_manat_demand' THEN t.financial_corporations_manat_demand
            WHEN 'financial_corporations_manat_time' THEN t.financial_corporations_manat_time
            WHEN 'financial_corporations_fx_demand' THEN t.financial_corporations_fx_demand
            WHEN 'financial_corporations_fx_time' THEN t.financial_corporations_fx_time
            
            -- Qeyri-Maliyyə Təşkilatları Detalları
            WHEN 'non_financial_corporations_manat_demand' THEN t.non_financial_corporations_manat_demand
            WHEN 'non_financial_corporations_manat_time' THEN t.non_financial_corporations_manat_time
            WHEN 'non_financial_corporations_fx_demand' THEN t.non_financial_corporations_fx_demand
            WHEN 'non_financial_corporations_fx_time' THEN t.non_financial_corporations_fx_time
        END AS amount
    FROM silver.depozit_ve_emtee_kredit t
    CROSS JOIN (
        -- 1. ÜMUMİ YEKUN (Toplanmaya daxil deyil)
        SELECT 'Portfolio Total' AS category_type, 'total_deposits' AS category_name, 'All' AS sector, 'All' AS currency, 'All' AS term_type, 1 AS is_total_metric UNION ALL
        
        -- 2. SEKTOR YEKUNLARI (Toplanmaya daxil deyil, sadəcə qruplaşdırma üçündür)
        SELECT 'Sector Total', 'households_total', 'Households', 'All', 'All', 1 UNION ALL
        SELECT 'Sector Total', 'financial_corporations_total', 'Financial Corporations', 'All', 'All', 1 UNION ALL
        SELECT 'Sector Total', 'non_financial_corporations_total', 'Non-Financial Corporations', 'All', 'All', 1 UNION ALL
        
        -- 3. ƏN AŞAĞI SƏVİYYƏ DETALLAR (Cəmləməyə yalnız bunlar daxildir: is_total_metric = 0)
        -- Households
        SELECT 'Deposit Component', 'households_manat_demand', 'Households', 'AZN', 'Demand', 0 UNION ALL
        SELECT 'Deposit Component', 'households_manat_time',   'Households', 'AZN', 'Time',   0 UNION ALL
        SELECT 'Deposit Component', 'households_fx_demand',    'Households', 'FX',  'Demand', 0 UNION ALL
        SELECT 'Deposit Component', 'households_fx_time',      'Households', 'FX',  'Time',   0 UNION ALL
        
        -- Financial Corporations
        SELECT 'Deposit Component', 'financial_corporations_manat_demand', 'Financial Corporations', 'AZN', 'Demand', 0 UNION ALL
        SELECT 'Deposit Component', 'financial_corporations_manat_time',   'Financial Corporations', 'AZN', 'Time',   0 UNION ALL
        SELECT 'Deposit Component', 'financial_corporations_fx_demand',    'Financial Corporations', 'FX',  'Demand', 0 UNION ALL
        SELECT 'Deposit Component', 'financial_corporations_fx_time',      'Financial Corporations', 'FX',  'Time',   0 UNION ALL
        
        -- Non-Financial Corporations
        SELECT 'Deposit Component', 'non_financial_corporations_manat_demand', 'Non-Financial Corporations', 'AZN', 'Demand', 0 UNION ALL
        SELECT 'Deposit Component', 'non_financial_corporations_manat_time',   'Non-Financial Corporations', 'AZN', 'Time',   0 UNION ALL
        SELECT 'Deposit Component', 'non_financial_corporations_fx_demand',    'Non-Financial Corporations', 'FX',  'Demand', 0 UNION ALL
        SELECT 'Deposit Component', 'non_financial_corporations_fx_time',      'Non-Financial Corporations', 'FX',  'Time',   0
    ) c
) final_data
WHERE amount IS NOT NULL;

-- SELECT * FROM gold.deposits_sector_dashboard_long;

-- ==============================================================================

DROP TABLE IF EXISTS gold.state_budget_dashboard_long;

CREATE TABLE gold.state_budget_dashboard_long AS
SELECT * 
FROM (
    SELECT 
        t.year,
        t.month,
        c.category_type,
        c.budget_component,
        CASE c.budget_component
            WHEN 'Revenue' THEN t.budget_revenue
            WHEN 'Expenses' THEN t.budget_expences
            WHEN 'Initial Balance' THEN t.budget_initial
        END AS amount_mln_manat
    FROM silver.dovlet_budcesi t
    CROSS JOIN (
        SELECT 'State Budget' AS category_type, 'Revenue' AS budget_component UNION ALL
        SELECT 'State Budget', 'Expenses' UNION ALL
        SELECT 'State Budget', 'Initial Balance'
    ) c
) final_data
WHERE amount_mln_manat IS NOT NULL;

-- SELECT * FROM gold.state_budget_dashboard_long LIMIT 100;

-- ==============================================================================

DROP TABLE IF EXISTS gold.exchange_rates_dashboard_long;

CREATE TABLE gold.exchange_rates_dashboard_long AS
SELECT * 
FROM (
    SELECT 
        t.year,
        t.month,
        c.rate_type,
        c.sector,
        c.is_total_metric,
        CASE c.original_column
            WHEN 'nominal_effektiv_mezenne_umumi' THEN t.nominal_effektiv_mezenne_umumi
            WHEN 'nem_qeyri_neft' THEN t.nem_qeyri_neft
            WHEN 'real_effektiv_mezenne_umumi' THEN t.real_effektiv_mezenne_umumi
            WHEN 'rem_qeyri_neft' THEN t.rem_qeyri_neft
        END AS index_value  -- QEYD: 'amount' yox, 'index_value' adlandırıldı
    FROM silver.effektiv_mezenne t
    CROSS JOIN (
        -- Ölçülərin (Dimensions) Xəritələnməsi
        -- Nominal Göstəricilər (NEER)
        SELECT 'Nominal' AS rate_type, 'Total' AS sector, 1 AS is_total_metric, 'nominal_effektiv_mezenne_umumi' AS original_column UNION ALL
        SELECT 'Nominal', 'Non-Oil', 0, 'nem_qeyri_neft' UNION ALL
        
        -- Real Göstəricilər (REER)
        SELECT 'Real', 'Total', 1, 'real_effektiv_mezenne_umumi' UNION ALL
        SELECT 'Real', 'Non-Oil', 0, 'rem_qeyri_neft'
    ) c
) final_data
WHERE index_value IS NOT NULL;

-- SELECT * FROM gold.exchange_rates_dashboard_long LIMIT 100;

-- ==============================================================================

DROP TABLE IF EXISTS gold.individual_savings_dashboard_long;

CREATE TABLE gold.individual_savings_dashboard_long AS
SELECT * 
FROM (
    SELECT 
        t.year,
        t.month,
        c.account_type,      -- 1-Cİ ÖLÇÜ: Əmanətin Növü (Transaction, Short-term, Long-term)
        c.currency,          -- 2-Cİ ÖLÇÜ: Valyuta (AZN, FX)
        c.residency,         -- 3-CÜ ÖLÇÜ: Rezidentlik (Residents, Non-Residents)
        c.is_total_metric,   -- YEKUN BAYRAĞI: 0 = Ən alt detal, 1 = Alt cəmlər və Yekunlar
        CASE c.original_column
            -- [1] ÜMUMİ YEKUNLAR
            WHEN 'total' THEN t.total
            WHEN 'total_national_currency' THEN t.total_national_currency
            WHEN 'total_foreign_currency' THEN t.total_foreign_currency
            WHEN 'residents' THEN t.residents
            WHEN 'residents_national_currency' THEN t.residents_national_currency
            WHEN 'residents_foreign_currency' THEN t.residents_foreign_currency
            WHEN 'nonresidents' THEN t.nonresidents
            WHEN 'nonresidents_national_currency' THEN t.nonresidents_national_currency
            WHEN 'nonresidents_foreign_currency' THEN t.nonresidents_foreign_currency
            
            -- [2] QISAMÜDDƏTLİ (Short-term)
            WHEN 'short_term_savings' THEN t.short_term_savings
            WHEN 'short_term_savings_national_currency' THEN t.short_term_savings_national_currency
            WHEN 'short_term_savings_national_currency_residents' THEN t.short_term_savings_national_currency_residents
            WHEN 'short_term_savings_national_currency_nonresidents' THEN t.short_term_savings_national_currency_nonresidents
            WHEN 'short_term_savings_foreign_currency' THEN t.short_term_savings_foreign_currency
            WHEN 'short_term_savings_foreign_currency_residents' THEN t.short_term_savings_foreign_currency_residents
            WHEN 'short_term_savings_foreign_currency_nonresidents' THEN t.short_term_savings_foreign_currency_nonresidents
            
            -- [3] TƏLƏB OLUNANADƏK / ƏMƏLİYYAT (Transaction)
            WHEN 'transaction_accounts' THEN t.transaction_accounts
            WHEN 'transaction_accounts_national_currency' THEN t.transaction_accounts_national_currency
            WHEN 'transaction_accounts_national_currency_residents' THEN t.transaction_accounts_national_currency_residents
            WHEN 'transaction_accounts_national_currency_nonresidents' THEN t.transaction_accounts_national_currency_nonresidents
            WHEN 'transaction_accounts_foreign_currency' THEN t.transaction_accounts_foreign_currency
            WHEN 'transaction_accounts_foreign_currency_residents' THEN t.transaction_accounts_foreign_currency_residents
            WHEN 'transaction_accounts_foreign_currency_nonresidents' THEN t.transaction_accounts_foreign_currency_nonresidents
            
            -- [4] UZUNMÜDDƏTLİ (Long-term)
            WHEN 'long_term_savings' THEN t.long_term_savings
            WHEN 'long_term_savings_national_currency' THEN t.long_term_savings_national_currency
            WHEN 'long_term_savings_national_currency_residents' THEN t.long_term_savings_national_currency_residents
            WHEN 'long_term_savings_national_currency_nonresidents' THEN t.long_term_savings_national_currency_nonresidents
            WHEN 'long_term_savings_foreign_currency' THEN t.long_term_savings_foreign_currency
            WHEN 'long_term_savings_foreign_currency_residents' THEN t.long_term_savings_foreign_currency_residents
            WHEN 'long_term_savings_foreign_currency_nonresidents' THEN t.long_term_savings_foreign_currency_nonresidents
        END AS amount
    FROM silver.fiziki_sexslerin_emtee_strukturu t
    CROSS JOIN (
        -- === ÜMUMİ YEKUNLAR VƏ ALT CƏMLƏR (is_total_metric = 1) ===
        SELECT 'total' AS original_column, 'All' AS account_type, 'All' AS currency, 'All' AS residency, 1 AS is_total_metric UNION ALL
        SELECT 'total_national_currency', 'All', 'AZN', 'All', 1 UNION ALL
        SELECT 'total_foreign_currency', 'All', 'FX', 'All', 1 UNION ALL
        SELECT 'residents', 'All', 'All', 'Residents', 1 UNION ALL
        SELECT 'residents_national_currency', 'All', 'AZN', 'Residents', 1 UNION ALL
        SELECT 'residents_foreign_currency', 'All', 'FX', 'Residents', 1 UNION ALL
        SELECT 'nonresidents', 'All', 'All', 'Non-Residents', 1 UNION ALL
        SELECT 'nonresidents_national_currency', 'All', 'AZN', 'Non-Residents', 1 UNION ALL
        SELECT 'nonresidents_foreign_currency', 'All', 'FX', 'Non-Residents', 1 UNION ALL
        
        -- Short-term Alt cəmlər
        SELECT 'short_term_savings', 'Short-term', 'All', 'All', 1 UNION ALL
        SELECT 'short_term_savings_national_currency', 'Short-term', 'AZN', 'All', 1 UNION ALL
        SELECT 'short_term_savings_foreign_currency', 'Short-term', 'FX', 'All', 1 UNION ALL
        
        -- Transaction Alt cəmlər
        SELECT 'transaction_accounts', 'Transaction', 'All', 'All', 1 UNION ALL
        SELECT 'transaction_accounts_national_currency', 'Transaction', 'AZN', 'All', 1 UNION ALL
        SELECT 'transaction_accounts_foreign_currency', 'Transaction', 'FX', 'All', 1 UNION ALL
        
        -- Long-term Alt cəmlər
        SELECT 'long_term_savings', 'Long-term', 'All', 'All', 1 UNION ALL
        SELECT 'long_term_savings_national_currency', 'Long-term', 'AZN', 'All', 1 UNION ALL
        SELECT 'long_term_savings_foreign_currency', 'Long-term', 'FX', 'All', 1 UNION ALL

        -- === ƏN ALT DETALLAR (is_total_metric = 0) - DAXİLİ TOPLANILA BİLƏN RƏQƏMLƏR ===
        SELECT 'short_term_savings_national_currency_residents', 'Short-term', 'AZN', 'Residents', 0 UNION ALL
        SELECT 'short_term_savings_national_currency_nonresidents', 'Short-term', 'AZN', 'Non-Residents', 0 UNION ALL
        SELECT 'short_term_savings_foreign_currency_residents', 'Short-term', 'FX', 'Residents', 0 UNION ALL
        SELECT 'short_term_savings_foreign_currency_nonresidents', 'Short-term', 'FX', 'Non-Residents', 0 UNION ALL
        
        SELECT 'transaction_accounts_national_currency_residents', 'Transaction', 'AZN', 'Residents', 0 UNION ALL
        SELECT 'transaction_accounts_national_currency_nonresidents', 'Transaction', 'AZN', 'Non-Residents', 0 UNION ALL
        SELECT 'transaction_accounts_foreign_currency_residents', 'Transaction', 'FX', 'Residents', 0 UNION ALL
        SELECT 'transaction_accounts_foreign_currency_nonresidents', 'Transaction', 'FX', 'Non-Residents', 0 UNION ALL
        
        SELECT 'long_term_savings_national_currency_residents', 'Long-term', 'AZN', 'Residents', 0 UNION ALL
        SELECT 'long_term_savings_national_currency_nonresidents', 'Long-term', 'AZN', 'Non-Residents', 0 UNION ALL
        SELECT 'long_term_savings_foreign_currency_residents', 'Long-term', 'FX', 'Residents', 0 UNION ALL
        SELECT 'long_term_savings_foreign_currency_nonresidents', 'Long-term', 'FX', 'Non-Residents', 0
    ) c
) final_data
WHERE amount IS NOT NULL;

-- SELECT * FROM gold.individual_savings_dashboard_long

-- ==============================================================================

DROP TABLE IF EXISTS gold.macro_indicators_dashboard_long;

CREATE TABLE gold.macro_indicators_dashboard_long AS
SELECT * 
FROM (
    SELECT 
        t.year,
        t.month,
        c.indicator_group,
        c.sector,
        c.metric_type,
        c.unit,
        CASE c.original_column
            -- ÜDM (GDP)
            WHEN 'gdp_total_mln_manat' THEN t.gdp_total_mln_manat
            WHEN 'gdp_growth_rate_percent' THEN t.gdp_growth_rate_percent
            WHEN 'gdp_deflator' THEN t.gdp_deflator
            WHEN 'non_oil_gdp_total_mln_manat' THEN t.non_oil_gdp_total_mln_manat
            WHEN 'non_oil_gdp_growth_rate_percent' THEN t.non_oil_gdp_growth_rate_percent
            
            -- Əsas Kapitala İnvestisiyalar
            WHEN 'capital_investments_total_mln_manat' THEN t.capital_investments_total_mln_manat
            WHEN 'capital_investments_growth_rate_percent' THEN t.capital_investments_growth_rate_percent
            
            -- Əhalinin Nominal Gəlirləri
            WHEN 'nominal_income_total_mln_manat' THEN t.nominal_income_total_mln_manat
            WHEN 'nominal_income_growth_rate_percent' THEN t.nominal_income_growth_rate_percent
            
            -- Orta Aylıq Əməkhaqqı
            WHEN 'average_monthly_wage_manat' THEN t.average_monthly_wage_manat
            WHEN 'average_monthly_wage_growth_rate_percent' THEN t.average_monthly_wage_growth_rate_percent
            
            -- İstehlak Qiymətləri İndeksi (İnflyasiya - CPI)
            WHEN 'cpi_monthly_percent' THEN t.cpi_monthly_percent
            WHEN 'cpi_12_month_percent' THEN t.cpi_12_month_percent
            WHEN 'cpi_annual_average_percent' THEN t.cpi_annual_average_percent
        END AS indicator_value
    FROM silver.macroiqtisadi_gostericiler t
    CROSS JOIN (
        -- === GDP ===
        SELECT 'gdp_total_mln_manat' AS original_column, 'GDP' AS indicator_group, 'Total' AS sector, 'Absolute' AS metric_type, 'Mln Manat' AS unit UNION ALL
        SELECT 'gdp_growth_rate_percent', 'GDP', 'Total', 'Growth Rate', '%' UNION ALL
        SELECT 'gdp_deflator', 'GDP', 'Total', 'Deflator', 'Index' UNION ALL
        SELECT 'non_oil_gdp_total_mln_manat', 'GDP', 'Non-Oil', 'Absolute', 'Mln Manat' UNION ALL
        SELECT 'non_oil_gdp_growth_rate_percent', 'GDP', 'Non-Oil', 'Growth Rate', '%' UNION ALL

        -- === İNVESTİSİYALAR ===
        SELECT 'capital_investments_total_mln_manat', 'Capital Investments', 'Total', 'Absolute', 'Mln Manat' UNION ALL
        SELECT 'capital_investments_growth_rate_percent', 'Capital Investments', 'Total', 'Growth Rate', '%' UNION ALL

        -- === NOMİNAL GƏLİRLƏR ===
        SELECT 'nominal_income_total_mln_manat', 'Nominal Income', 'Total', 'Absolute', 'Mln Manat' UNION ALL
        SELECT 'nominal_income_growth_rate_percent', 'Nominal Income', 'Total', 'Growth Rate', '%' UNION ALL

        -- === ƏMƏKHAQQI ===
        SELECT 'average_monthly_wage_manat', 'Wages', 'Total', 'Absolute', 'Manat' UNION ALL
        SELECT 'average_monthly_wage_growth_rate_percent', 'Wages', 'Total', 'Growth Rate', '%' UNION ALL

        -- === CPI (İNFLYASİYA) ===
        SELECT 'cpi_monthly_percent', 'CPI (Inflation)', 'Total', 'Monthly Rate', '%' UNION ALL
        SELECT 'cpi_12_month_percent', 'CPI (Inflation)', 'Total', '12-Month Rate', '%' UNION ALL
        SELECT 'cpi_annual_average_percent', 'CPI (Inflation)', 'Total', 'Annual Average Rate', '%'
    ) c
) final_data
WHERE indicator_value IS NOT NULL;

-- SELECT * FROM gold.macro_indicators_dashboard_long

-- ==============================================================================

DROP TABLE IF EXISTS gold.npl_dashboard_long;

CREATE TABLE gold.npl_dashboard_long AS
SELECT * 
FROM (
    SELECT 
        t.year,
        t.month,
        c.loan_type,         -- 1-Cİ ÖLÇÜ: Kreditin Növü (Total, Business, Consumer, Mortgage)
        c.metric_type,       -- 2-Cİ ÖLÇÜ: Metrikin Növü (Absolute, Ratio)
        c.unit,              -- 3-CÜ ÖLÇÜ: Vahid (Məbləğ, %)
        c.is_total_metric,   -- YEKUN BAYRAĞI: 1 = Total Portfolio, 0 = Alt qruplar
        CASE c.original_column
            -- Mütləq Rəqəmlər (Məbləğ)
            WHEN 'npl_total' THEN t.npl_total
            WHEN 'npl_business_loans' THEN t.npl_business_loans
            WHEN 'npl_consumer_loans' THEN t.npl_consumer_loans
            WHEN 'npl_mortgage_loans' THEN t.npl_mortgage_loans
            
            -- Faiz Göstəriciləri (NPL Ratio)
            WHEN 'npl_ratio_total' THEN t.npl_ratio_total
            WHEN 'npl_ratio_business' THEN t.npl_ratio_business
            WHEN 'npl_ratio_consumer' THEN t.npl_ratio_consumer
            WHEN 'npl_ratio_mortgage' THEN t.npl_ratio_mortgage
        END AS metric_value
    FROM silver.npl_structure t
    CROSS JOIN (
        -- === MÜTLƏQ RƏQƏMLƏR (Absolute Values) ===
        SELECT 'npl_total' AS original_column, 'Total Portfolio' AS loan_type, 'Absolute' AS metric_type, 'Amount' AS unit, 1 AS is_total_metric UNION ALL
        SELECT 'npl_business_loans', 'Business', 'Absolute', 'Amount', 0 UNION ALL
        SELECT 'npl_consumer_loans', 'Consumer', 'Absolute', 'Amount', 0 UNION ALL
        SELECT 'npl_mortgage_loans', 'Mortgage', 'Absolute', 'Amount', 0 UNION ALL
        
        -- === NPL FAİZLƏRİ (Ratios) ===
        SELECT 'npl_ratio_total', 'Total Portfolio', 'Ratio', '%', 1 UNION ALL
        SELECT 'npl_ratio_business', 'Business', 'Ratio', '%', 0 UNION ALL
        SELECT 'npl_ratio_consumer', 'Consumer', 'Ratio', '%', 0 UNION ALL
        SELECT 'npl_ratio_mortgage', 'Mortgage', 'Ratio', '%', 0
    ) c
) final_data
WHERE metric_value IS NOT NULL;

-- SELECT * FROM gold.npl_dashboard_long LIMIT 100;

-- ==============================================================================

DROP TABLE IF EXISTS gold.bop_foreign_trade_dashboard_long;

CREATE TABLE gold.bop_foreign_trade_dashboard_long AS
SELECT * 
FROM (
    SELECT 
        t.year,
        t.month,
        c.account_group,   -- 1-Cİ ÖLÇÜ: Cari hesab, Maliyyə hesabı və s.
        c.component,       -- 2-Cİ ÖLÇÜ: Əmtəə, Xidmət, İnvestisiya və s.
        c.flow_type,       -- 3-CÜ ÖLÇÜ: İxrac/Daxilolma, İdxal/Ödəniş, Balans
        c.sector,          -- 4-CÜ ÖLÇÜ: Neft-qaz, Qeyri-neft, Ümumi
        c.is_total_metric, -- YEKUN BAYRAĞI: İkiqat hesablamanın qarşısını almaq üçün
        CASE c.original_column
            -- [1] CARİ HESAB VƏ ƏMTƏƏLƏR
            WHEN 'current_account' THEN t.current_account
            WHEN 'foreign_trade_balance' THEN t.foreign_trade_balance
            WHEN 'export_of_goods' THEN t.export_of_goods
            WHEN 'export_of_goods_oil_and_gas_sector' THEN t.export_of_goods_oil_and_gas_sector
            WHEN 'export_of_goods_other_sectors' THEN t.export_of_goods_other_sectors
            WHEN 'import_of_goods' THEN t.import_of_goods
            WHEN 'import_of_goods_oil_and_gas_sector' THEN t.import_of_goods_oil_and_gas_sector
            WHEN 'import_of_goods_other_sectors' THEN t.import_of_goods_other_sectors
            
            -- [2] XİDMƏTLƏR BALANSI
            WHEN 'balance_of_services' THEN t.balance_of_services
            WHEN 'balance_of_services_oil_and_gas_sector' THEN t.balance_of_services_oil_and_gas_sector
            WHEN 'balance_of_services_other_sectors' THEN t.balance_of_services_other_sectors
            WHEN 'services_transport' THEN t.services_transport
            WHEN 'services_construction' THEN t.services_construction
            
            -- [3] İLKİN GƏLİR
            WHEN 'primary_income' THEN t.primary_income
            WHEN 'primary_income_oil_and_gas_sector' THEN t.primary_income_oil_and_gas_sector
            WHEN 'primary_income_other_sectors' THEN t.primary_income_other_sectors
            WHEN 'primary_income_receipts' THEN t.primary_income_receipts
            WHEN 'primary_income_payments' THEN t.primary_income_payments
            
            -- [4] TƏKRAR GƏLİR
            WHEN 'secondary_income' THEN t.secondary_income
            WHEN 'secondary_income_remittances' THEN t.secondary_income_remittances
            WHEN 'secondary_income_receipts' THEN t.secondary_income_receipts
            WHEN 'secondary_income_payments' THEN t.secondary_income_payments
            
            -- [5] KAPİTAL VƏ MALİYYƏ HESABLARI
            WHEN 'capital_account' THEN t.capital_account
            WHEN 'financial_account' THEN t.financial_account
            WHEN 'financial_account_net_acquisition_of_financial_assets' THEN t.financial_account_net_acquisition_of_financial_assets
            WHEN 'financial_account_direct_investment_abroad' THEN t.financial_account_direct_investment_abroad
            WHEN 'financial_account_portfolio_and_other_investments_assets' THEN t.financial_account_portfolio_and_other_investments_assets
            WHEN 'financial_account_net_incurrence_of_liabilities' THEN t.financial_account_net_incurrence_of_liabilities
            WHEN 'financial_account_direct_investment_in_azerbaijan' THEN t.financial_account_direct_investment_in_azerbaijan
            WHEN 'financial_account_repatriation_of_investments' THEN t.financial_account_repatriation_of_investments
            WHEN 'financial_account_oil_bonus' THEN t.financial_account_oil_bonus
            WHEN 'financial_account_portfolio_and_other_investments_liabilities' THEN t.financial_account_portfolio_and_other_investments_liabilities
            
            -- [6] EHTİYATLAR VƏ XƏTALAR
            WHEN 'net_errors_and_omissions' THEN t.net_errors_and_omissions
            WHEN 'changes_in_reserve_assets' THEN t.changes_in_reserve_assets
        END AS amount_usd
    FROM silver.xarici_ticaret t
    CROSS JOIN (
        -- MAPPING CƏDVƏLİ (Dimensions)
        
        -- Cari Hesab (Current Account) - Əmtəələr
        SELECT 'current_account' AS original_column, 'Current Account' AS account_group, 'Total' AS component, 'Balance' AS flow_type, 'Total' AS sector, 1 AS is_total_metric UNION ALL
        SELECT 'foreign_trade_balance', 'Current Account', 'Goods', 'Balance', 'Total', 1 UNION ALL
        SELECT 'export_of_goods', 'Current Account', 'Goods', 'Export/Receipts', 'Total', 1 UNION ALL
        SELECT 'export_of_goods_oil_and_gas_sector', 'Current Account', 'Goods', 'Export/Receipts', 'Oil & Gas', 0 UNION ALL
        SELECT 'export_of_goods_other_sectors', 'Current Account', 'Goods', 'Export/Receipts', 'Other Sectors', 0 UNION ALL
        SELECT 'import_of_goods', 'Current Account', 'Goods', 'Import/Payments', 'Total', 1 UNION ALL
        SELECT 'import_of_goods_oil_and_gas_sector', 'Current Account', 'Goods', 'Import/Payments', 'Oil & Gas', 0 UNION ALL
        SELECT 'import_of_goods_other_sectors', 'Current Account', 'Goods', 'Import/Payments', 'Other Sectors', 0 UNION ALL
        
        -- Cari Hesab - Xidmətlər
        SELECT 'balance_of_services', 'Current Account', 'Services', 'Balance', 'Total', 1 UNION ALL
        SELECT 'balance_of_services_oil_and_gas_sector', 'Current Account', 'Services', 'Balance', 'Oil & Gas', 0 UNION ALL
        SELECT 'balance_of_services_other_sectors', 'Current Account', 'Services', 'Balance', 'Other Sectors', 1 UNION ALL
        SELECT 'services_transport', 'Current Account', 'Services (Transport)', 'Balance', 'Other Sectors', 0 UNION ALL
        SELECT 'services_construction', 'Current Account', 'Services (Construction)', 'Balance', 'Other Sectors', 0 UNION ALL
        
        -- Cari Hesab - İlkin Gəlir
        SELECT 'primary_income', 'Current Account', 'Primary Income', 'Balance', 'Total', 1 UNION ALL
        SELECT 'primary_income_oil_and_gas_sector', 'Current Account', 'Primary Income', 'Balance', 'Oil & Gas', 0 UNION ALL
        SELECT 'primary_income_other_sectors', 'Current Account', 'Primary Income', 'Balance', 'Other Sectors', 0 UNION ALL
        SELECT 'primary_income_receipts', 'Current Account', 'Primary Income', 'Export/Receipts', 'Total', 1 UNION ALL
        SELECT 'primary_income_payments', 'Current Account', 'Primary Income', 'Import/Payments', 'Total', 1 UNION ALL
        
        -- Cari Hesab - Təkrar Gəlir
        SELECT 'secondary_income', 'Current Account', 'Secondary Income', 'Balance', 'Total', 1 UNION ALL
        SELECT 'secondary_income_remittances', 'Current Account', 'Secondary Income (Remittances)', 'Balance', 'Total', 0 UNION ALL
        SELECT 'secondary_income_receipts', 'Current Account', 'Secondary Income', 'Export/Receipts', 'Total', 1 UNION ALL
        SELECT 'secondary_income_payments', 'Current Account', 'Secondary Income', 'Import/Payments', 'Total', 1 UNION ALL
        
        -- Kapital və Maliyyə Hesabları
        SELECT 'capital_account', 'Capital Account', 'Capital Account Total', 'Balance', 'Total', 1 UNION ALL
        SELECT 'financial_account', 'Financial Account', 'Financial Account Total', 'Balance', 'Total', 1 UNION ALL
        
        SELECT 'financial_account_net_acquisition_of_financial_assets', 'Financial Account', 'Assets', 'Net Acquisition', 'Total', 1 UNION ALL
        SELECT 'financial_account_direct_investment_abroad', 'Financial Account', 'Direct Investment', 'Net Acquisition', 'Total', 0 UNION ALL
        SELECT 'financial_account_portfolio_and_other_investments_assets', 'Financial Account', 'Portfolio & Other', 'Net Acquisition', 'Total', 0 UNION ALL
        
        SELECT 'financial_account_net_incurrence_of_liabilities', 'Financial Account', 'Liabilities', 'Net Incurrence', 'Total', 1 UNION ALL
        SELECT 'financial_account_direct_investment_in_azerbaijan', 'Financial Account', 'Direct Investment', 'Net Incurrence', 'Total', 0 UNION ALL
        SELECT 'financial_account_repatriation_of_investments', 'Financial Account', 'Repatriation', 'Net Incurrence', 'Total', 0 UNION ALL
        SELECT 'financial_account_oil_bonus', 'Financial Account', 'Oil Bonus', 'Net Incurrence', 'Oil & Gas', 0 UNION ALL
        SELECT 'financial_account_portfolio_and_other_investments_liabilities', 'Financial Account', 'Portfolio & Other', 'Net Incurrence', 'Total', 0 UNION ALL
        
        -- Ehtiyatlar və Xətalar
        SELECT 'net_errors_and_omissions', 'Balancing Items', 'Net Errors & Omissions', 'Balance', 'Total', 1 UNION ALL
        SELECT 'changes_in_reserve_assets', 'Balancing Items', 'Reserve Assets', 'Balance', 'Total', 1
    ) c
) final_data
WHERE amount_usd IS NOT NULL;

SELECT * FROM gold.bop_foreign_trade_dashboard_long;