CREATE
OR REPLACE VIEW "matillion"."prod"."salesforce_events_r12" AS WITH MonthlySales AS (
    SELECT
        TO_DATE(LSS.trx_month, 'Mon YYYY') AS monthyear,
        -- Convert transaction month to date
        LSS.region AS
    region
,
        LSS.countryname AS country,
        LSS.managername AS managername,
        LSS.salesrepnumber AS salesrepnumber,
        LSS.salesrepname AS salesrepname,
        LSS.name AS name,
        LSS.account_type AS account_type,
        SUM(LSS.visit_count) AS visit_count,
        SUM(LSS.appointment_count) AS appointment_count,
        SUM(LSS.coldcall_count) AS coldcall_count,
        SUM(LSS.qb_seen_count) AS qb_seen_count,
        SUM(LSS.demo_count) AS demo_count,
        SUM(LSS.survey_count) AS survey_count,
        SUM(LSS.offer_count) AS offer_count,
        SUM(LSS.order_count) AS order_count,
        SUM(LSS.record_count) AS record_count
    FROM
        (
            -- Step 2: Fill Missing Data and Join with Date Reference
            SELECT
                COALESCE(MST.visit_count, 0) AS visit_count,
                COALESCE(MST.appointment_count, 0) AS appointment_count,
                COALESCE(MST.coldcall_count, 0) AS coldcall_count,
                COALESCE(MST.qb_seen_count, 0) AS qb_seen_count,
                COALESCE(MST.demo_count, 0) AS demo_count,
                COALESCE(MST.survey_count, 0) AS survey_count,
                COALESCE(MST.offer_count, 0) AS offer_count,
                COALESCE(MST.order_count, 0) AS order_count,
                COALESCE(MST.record_count, 0) AS record_count,
                -- Handle missing revenue
                DRIVER.region AS
            region
,
                DRIVER.countryname AS countryname,
                DRIVER.managername AS managername,
                DRIVER.salesrepnumber AS salesrepnumber,
                DRIVER.salesrepname AS salesrepname,
                DRIVER.name AS name,
                DRIVER.account_type AS account_type,
                TO_CHAR(DRIVER.date_actual, 'Mon YYYY') AS trx_month
            FROM
                (
                    -- Generate a list of dates and join with the sales rep data
                    SELECT
                        D.date_actual,
                        MST.region,
                        MST.countryname,
                        MST.managername,
                        MST.salesrepnumber,
                        MST.salesrepname,
                        MST.name,
                        MST.account_type
                    FROM
                        (
                            -- Generate a list of dates up to the current date
                            SELECT
                                TO_DATE(date_actual:: date, 'YYYY MM DD') AS date_actual
                            FROM
                                matillion.spectrum_reference.dim_date
                        ) D
                        CROSS JOIN (
                            SELECT
                                DISTINCT
                            region
,
                                countryname,
                                managername,
                                salesrepnumber,
                                salesrepname,
                                name,
                                account_type
                            FROM
                                "matillion"."prod"."salesforce_eventscount"
                        ) MST
                ) DRIVER
                LEFT JOIN "matillion"."prod"."salesforce_eventscount" MST ON TO_DATE(MST.month_year, 'Mon YYYY') = TO_DATE(DRIVER.date_actual, 'YYYY-MM-DD')
                AND DRIVER.salesrepnumber = MST.salesrepnumber
                AND DRIVER.salesrepname = MST.salesrepname
                AND DRIVER.name = MST.name
                AND DRIVER.account_type = MST.account_type
        ) LSS
    GROUP BY
    region
,
        country,
        managername,
    region
,
        countryname,
        managername,
        monthyear,
        salesrepnumber,
        salesrepname,
        name,
        account_type
)
SELECT
    monthyear,
region
,
    country,
    managername,
    -- Convert transaction month to date
    salesrepnumber,
    salesrepname,
    name,
    account_type,
    visit_count,
    appointment_count,
    Coldcall_count,
    qb_seen_count,
    demo_count,
    survey_count,
    offer_count,
    order_count,
    record_count,
    SUM(visit_count) OVER (
        PARTITION BY salesrepnumber,
        salesrepname,
        name,
        account_type
        ORDER BY
            monthyear ASC ROWS BETWEEN 11 PRECEDING
            AND CURRENT ROW
    ) AS visit_count_ttm,
    SUM(appointment_count) OVER (
        PARTITION BY salesrepnumber,
        salesrepname,
        name,
        account_type
        ORDER BY
            monthyear ASC ROWS BETWEEN 11 PRECEDING
            AND CURRENT ROW
    ) AS appointment_count_ttm,
    SUM(coldcall_count) OVER (
        PARTITION BY salesrepnumber,
        salesrepname,
        name,
        account_type
        ORDER BY
            monthyear ASC ROWS BETWEEN 11 PRECEDING
            AND CURRENT ROW
    ) AS coldcall_count_ttm,
    SUM(qb_seen_count) OVER (
        PARTITION BY salesrepnumber,
        salesrepname,
        name,
        account_type
        ORDER BY
            monthyear ASC ROWS BETWEEN 11 PRECEDING
            AND CURRENT ROW
    ) AS qb_seen_count_ttm,
    SUM(demo_count) OVER (
        PARTITION BY salesrepnumber,
        salesrepname,
        name,
        account_type
        ORDER BY
            monthyear ASC ROWS BETWEEN 11 PRECEDING
            AND CURRENT ROW
    ) AS demo_count_ttm,
    SUM(survey_count) OVER (
        PARTITION BY salesrepnumber,
        salesrepname,
        name,
        account_type
        ORDER BY
            monthyear ASC ROWS BETWEEN 11 PRECEDING
            AND CURRENT ROW
    ) AS survey_count_ttm,
    SUM(offer_count) OVER (
        PARTITION BY salesrepnumber,
        salesrepname,
        name,
        account_type
        ORDER BY
            monthyear ASC ROWS BETWEEN 11 PRECEDING
            AND CURRENT ROW
    ) AS offer_count_ttm,
    SUM(order_count) OVER (
        PARTITION BY salesrepnumber,
        salesrepname,
        name,
        account_type
        ORDER BY
            monthyear ASC ROWS BETWEEN 11 PRECEDING
            AND CURRENT ROW
    ) AS order_count_ttm,
    SUM(record_count) OVER (
        PARTITION BY salesrepnumber,
        salesrepname,
        name,
        account_type
        ORDER BY
            monthyear ASC ROWS BETWEEN 11 PRECEDING
            AND CURRENT ROW
    ) AS record_count_ttm
FROM
    MonthlySales
ORDER BY
    monthyear,
region
,
    country,
    managername,
    -- Convert transaction month to date
    salesrepname,
    name WITH NO SCHEMA BINDING;