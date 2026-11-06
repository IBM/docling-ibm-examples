-- The events — one statement, from the figures of a filing to what changed in it.
--
--     metrics ──▶ financials (one row per filing) ──▶ five detectors ──▶ events
--
-- Nothing here calls Docling: it reads the metrics topic, which is what was
-- paid for, so a threshold can be changed and this redeployed for free — the
-- events of every filing already read are derived again.
--
-- Each detector compares the figure of a filing with the one beside it in the
-- same statement (`prior_value`), so it needs one filing and no other. What
-- that figure is — the same quarter of last year, the last year-end — is in the
-- row's `comparison`.
--
--   net_income_negative            net income was positive and is a loss now.          high
--   operating_cash_flow_negative   as above, for cash from operating activities.       high
--   gross_margin_compression       gross margin fell by EVENT_MARGIN_DROP or more      medium,
--                                  (percentage points).                                 high from EVENT_MARGIN_DROP_HIGH
--   costs_outpacing_revenue        operating costs (revenue less operating income)     medium,
--                                  grew by EVENT_COST_GAP or more points faster         high from EVENT_COST_GAP_HIGH
--                                  than revenue, and did grow.
--   cash_drop                      cash fell by EVENT_CASH_DROP or more (per cent).    medium,
--                                                                                       high from EVENT_CASH_DROP_HIGH
--
-- The thresholds are in env.sh; they are part of this statement's fingerprint,
-- so changing one replaces it. A figure a filing does not have is not an event:
-- a bank or a BDC has no revenue, so it can have no margin or cost event, only
-- the net income, cash flow and cash ones.
--
-- ## What it writes
--
-- An upsert table keyed by filing and event: a filing read again replaces its
-- events, and the one a corrected figure no longer has is removed.
--
-- ## Layers
--
-- 1. the detectors, a UNION ALL: one row per event, in numbers (dollars, or
--    per cent for a margin);
-- 2. the numbers as text — "$12.3M", "41.0%";
-- 3. the sentence.
INSERT INTO `${EVENTS_TOPIC}`
  (`key`, `doc_id`, `company_name`, `cik`, `form`, `period_end`, `filing_date`, `event_type`, `severity`,
   `description`, `value_now`, `value_before`, `unit`, `delta`, `comparison`, `source_url`)
SELECT
  ENCODE(CONCAT(`doc_id`, ':', `event_type`), 'UTF-8')   AS `key`,
  `doc_id`, `company_name`, `cik`, `form`, `period_end`, `filing_date`, `event_type`, `severity`,
  CONCAT(`headline`, ': ', `text_now`, ' from ', `text_before`,
         CASE WHEN `delta` IS NULL THEN CAST('' AS STRING) ELSE CONCAT(' (', `delta_text`, ')') END,
         CASE WHEN `comparison` IS NULL THEN CAST('' AS STRING) ELSE CONCAT(' [', `comparison`, ']') END) AS `description`,
  `value_now`, `value_before`, `unit`, `delta`, `comparison`, `source_url`
FROM (
  SELECT
    *,
    -- 2. the numbers as text
    CASE WHEN `unit` = '%' THEN CONCAT(CAST(ROUND(`value_now`, 1) AS STRING), '%')
         ELSE CONCAT(CASE WHEN `value_now` < 0 THEN CAST('-' AS STRING) ELSE CAST('' AS STRING) END, '$',
              CASE WHEN ABS(`value_now`) >= 1000000000 THEN CONCAT(CAST(ROUND(ABS(`value_now`) / 1000000000, 2) AS STRING), 'B')
                   WHEN ABS(`value_now`) >= 1000000    THEN CONCAT(CAST(ROUND(ABS(`value_now`) / 1000000, 1) AS STRING), 'M')
                   ELSE CONCAT(CAST(ROUND(ABS(`value_now`) / 1000, 1) AS STRING), 'K') END)
    END AS `text_now`,
    CASE WHEN `unit` = '%' THEN CONCAT(CAST(ROUND(`value_before`, 1) AS STRING), '%')
         ELSE CONCAT(CASE WHEN `value_before` < 0 THEN CAST('-' AS STRING) ELSE CAST('' AS STRING) END, '$',
              CASE WHEN ABS(`value_before`) >= 1000000000 THEN CONCAT(CAST(ROUND(ABS(`value_before`) / 1000000000, 2) AS STRING), 'B')
                   WHEN ABS(`value_before`) >= 1000000    THEN CONCAT(CAST(ROUND(ABS(`value_before`) / 1000000, 1) AS STRING), 'M')
                   ELSE CONCAT(CAST(ROUND(ABS(`value_before`) / 1000, 1) AS STRING), 'K') END)
    END AS `text_before`,
    CASE WHEN `unit` = '%' THEN CONCAT(CAST(ROUND(`delta`, 1) AS STRING), ' points')
         ELSE CONCAT(CAST(ROUND(`delta`, 1) AS STRING), '%')
    END AS `delta_text`
  FROM (

    -- 1. the detectors ------------------------------------------------------

    -- net income turned into a loss
    SELECT `doc_id`, `company_name`, `cik`, `form`, `period_end`, `filing_date`, `source_url`,
           CAST('net_income_negative' AS STRING) AS `event_type`, CAST('high' AS STRING) AS `severity`,
           CAST('Net income turned negative' AS STRING) AS `headline`,
           `net_income` AS `value_now`, `net_income_prior` AS `value_before`, CAST('USD' AS STRING) AS `unit`,
           CAST(NULL AS DOUBLE) AS `delta`, `net_income_cmp` AS `comparison`
    FROM `${VIEW_FINANCIALS}`
    WHERE `net_income` < 0 AND `net_income_prior` > 0

    UNION ALL

    -- cash from operations turned negative
    SELECT `doc_id`, `company_name`, `cik`, `form`, `period_end`, `filing_date`, `source_url`,
           CAST('operating_cash_flow_negative' AS STRING), CAST('high' AS STRING),
           CAST('Operating cash flow turned negative' AS STRING),
           `operating_cash_flow`, `operating_cash_flow_prior`, CAST('USD' AS STRING),
           CAST(NULL AS DOUBLE), `operating_cash_flow_cmp`
    FROM `${VIEW_FINANCIALS}`
    WHERE `operating_cash_flow` < 0 AND `operating_cash_flow_prior` > 0

    UNION ALL

    -- gross margin fell: values are per cent, the delta is in points
    SELECT `doc_id`, `company_name`, `cik`, `form`, `period_end`, `filing_date`, `source_url`,
           CAST('gross_margin_compression' AS STRING),
           CASE WHEN `gross_margin_prior` - `gross_margin` >= ${EVENT_MARGIN_DROP_HIGH} THEN CAST('high' AS STRING) ELSE CAST('medium' AS STRING) END,
           CAST('Gross margin compressed' AS STRING),
           `gross_margin` * 100, `gross_margin_prior` * 100, CAST('%' AS STRING),
           (`gross_margin` - `gross_margin_prior`) * 100, `gross_profit_cmp`
    FROM `${VIEW_FINANCIALS}`
    WHERE `gross_margin_prior` - `gross_margin` >= ${EVENT_MARGIN_DROP}

    UNION ALL

    -- costs grew faster than revenue: values are the growth rates in per cent, the delta is the gap in points
    SELECT `doc_id`, `company_name`, `cik`, `form`, `period_end`, `filing_date`, `source_url`,
           CAST('costs_outpacing_revenue' AS STRING),
           CASE WHEN `cost_growth` - `revenue_growth` >= ${EVENT_COST_GAP_HIGH} THEN CAST('high' AS STRING) ELSE CAST('medium' AS STRING) END,
           CAST('Operating costs growing faster than revenue' AS STRING),
           `cost_growth` * 100, `revenue_growth` * 100, CAST('%' AS STRING),
           (`cost_growth` - `revenue_growth`) * 100, `revenue_cmp`
    FROM `${VIEW_FINANCIALS}`
    WHERE `cost_growth` > 0 AND `cost_growth` - `revenue_growth` >= ${EVENT_COST_GAP}

    UNION ALL

    -- cash fell sharply: the delta is the change in per cent
    SELECT `doc_id`, `company_name`, `cik`, `form`, `period_end`, `filing_date`, `source_url`,
           CAST('cash_drop' AS STRING),
           CASE WHEN `cash_change` <= -${EVENT_CASH_DROP_HIGH} THEN CAST('high' AS STRING) ELSE CAST('medium' AS STRING) END,
           CAST('Cash fell sharply' AS STRING),
           `cash`, `cash_prior`, CAST('USD' AS STRING),
           `cash_change` * 100, `cash_cmp`
    FROM `${VIEW_FINANCIALS}`
    WHERE `cash_change` <= -${EVENT_CASH_DROP}
  )
);
