
WITH PRE_HISTORY AS (SELECT
        UpdateYearWeekKey,
        GeographyKey,
        ClusterKey, 
        ProductKey as model,
        RIGHT(ProductVariantsKey, 8) as grid,
        EventKey,
        RIGHT(UpdateYearWeekKey,2) as WK, 
        ProductionSchedulerKey,
 
        --riportiamo target per glieventi in colonna
        -- PQ1
        SUM(CASE WHEN EventKey='PQ1' THEN EventQuantity ELSE 0 END) AS EventQty_PQ1,
        SUM(CASE WHEN EventKey='PQ1' THEN ChargedPostWeekN ELSE 0 END) AS ChargedPostWeekN_PQ1, --Cambiare PreWeek con PostWeekN
        SUM(CASE WHEN EventKey='PQ1' THEN ChargedPreWeekN ELSE 0 END) AS ChargedPreWeekN_PQ1,
        CASE WHEN EventKey='PQ1' THEN EventYearWeekKey ELSE 0 END AS EventYearWeek_PQ1,
        CASE WHEN EventKey='PQ1' THEN ExclusivityKey ELSE NULL END AS ExclusivityKey_PQ1, -- Canale ma non è affi dabile
         -- PQ2
        SUM(CASE WHEN EventKey='PQ2' THEN EventQuantity ELSE 0 END) AS EventQty_PQ2,
        SUM(CASE WHEN EventKey='PQ2' THEN ChargedPostWeekN ELSE 0 END) AS ChargedPostWeekN_PQ2,
        SUM(CASE WHEN EventKey='PQ2' THEN ChargedPreWeekN ELSE 0 END) AS ChargedPreWeekN_PQ2,
        CASE WHEN EventKey='PQ2' THEN EventYearWeekKey ELSE 0 END AS EventYearWeek_PQ2,
        CASE WHEN EventKey='PQ2' THEN ExclusivityKey ELSE NULL END AS ExclusivityKey_PQ2,
        -- PQ3
        SUM(CASE WHEN EventKey='PQ3' THEN EventQuantity ELSE 0 END) AS EventQty_PQ3,
        SUM(CASE WHEN EventKey='PQ3' THEN ChargedPostWeekN ELSE 0 END) AS ChargedPostWeekN_PQ3,
        SUM(CASE WHEN EventKey='PQ3' THEN ChargedPreWeekN ELSE 0 END) AS ChargedPreWeekN_PQ3,
        CASE WHEN EventKey='PQ3' THEN EventYearWeekKey ELSE 0 END AS EventYearWeek_PQ3,
        CASE WHEN EventKey='PQ3' THEN ExclusivityKey ELSE NULL END AS ExclusivityKey_PQ3,
        -- PQ4
        SUM(CASE WHEN EventKey='PQ4' THEN EventQuantity ELSE 0 END) AS EventQty_PQ4,
        SUM(CASE WHEN EventKey='PQ4' THEN ChargedPostWeekN ELSE 0 END) AS ChargedPostWeekN_PQ4,
        SUM(CASE WHEN EventKey='PQ4' THEN ChargedPreWeekN ELSE 0 END) AS ChargedPreWeekN_PQ4,
        CASE WHEN EventKey='PQ4' THEN EventYearWeekKey ELSE 0 END AS EventYearWeek_PQ4,
        CASE WHEN EventKey='PQ4' THEN ExclusivityKey ELSE NULL END AS ExclusivityKey_PQ4,
        -- PQ5  
        SUM(CASE WHEN EventKey='PQ5' THEN EventQuantity ELSE 0 END) AS EventQty_PQ5,
        SUM(CASE WHEN EventKey='PQ5' THEN ChargedPostWeekN ELSE 0 END) AS ChargedPostWeekN_PQ5,
        SUM(CASE WHEN EventKey='PQ5' THEN ChargedPreWeekN ELSE 0 END) AS ChargedPreWeekN_PQ5,
        CASE WHEN EventKey='PQ5' THEN EventYearWeekKey ELSE 0 END AS EventYearWeek_PQ5,
        CASE WHEN EventKey='PQ5' THEN ExclusivityKey ELSE NULL END AS ExclusivityKey_PQ5,
        -- PQ
        SUM(CASE WHEN EventKey='PQ' THEN EventQuantity ELSE 0 END) AS EventQty_PQ,
        SUM(CASE WHEN EventKey='PQ' THEN ChargedPostWeekN ELSE 0 END) AS ChargedPostWeekN_PQ,
        SUM(CASE WHEN EventKey='PQ' THEN ChargedPreWeekN ELSE 0 END) AS ChargedPreWeekN_PQ,
        CASE WHEN EventKey='PQ' THEN EventYearWeekKey ELSE 0 END AS EventYearWeek_PQ,
        CASE WHEN EventKey='PQ' THEN ExclusivityKey ELSE NULL END AS ExclusivityKey_PQ,
        -- FQ1
        SUM(CASE WHEN EventKey='FQ1' THEN EventQuantity ELSE 0 END) AS EventQty_FQ1,
        SUM(CASE WHEN EventKey='FQ1' THEN ChargedPostWeekN ELSE 0 END) AS ChargedPostWeekN_FQ1,
        SUM(CASE WHEN EventKey='FQ1' THEN ChargedPreWeekN ELSE 0 END) AS ChargedPreWeekN_FQ1,
        CASE WHEN EventKey='FQ1' THEN EventYearWeekKey ELSE 0 END AS EventYearWeek_FQ1,
        CASE WHEN EventKey='FQ1' THEN ExclusivityKey ELSE NULL END AS ExclusivityKey_FQ1,
        -- FQ2
        SUM(CASE WHEN EventKey='FQ2' THEN EventQuantity ELSE 0 END) AS EventQty_FQ2,
        SUM(CASE WHEN EventKey='FQ2' THEN ChargedPostWeekN ELSE 0 END) AS ChargedPostWeekN_FQ2,
        SUM(CASE WHEN EventKey='FQ2' THEN ChargedPreWeekN ELSE 0 END) AS ChargedPreWeekN_FQ2,
        CASE WHEN EventKey='FQ2' THEN EventYearWeekKey ELSE 0 END AS EventYearWeek_FQ2,
        CASE WHEN EventKey='FQ2' THEN ExclusivityKey ELSE NULL END AS ExclusivityKey_FQ2,
 
        --VERSATO LORDO (RIPETUTO UGUALE PER TUTTE LE RIGHE/SKU DEL DB)
        AVG(TotalChargedQuantity) as TotalChargedQuantity,
        AVG(TotalOrderedQuantity) as TotalOrderedQuantity,
       --qtà lorda in portafoglio ordini carta=non ancora startato in fabbrica
        AVG([5 WipQuantity]) as PaperQuantity
 
    FROM [NPI].[MODEL].[vFactSpreadingEvents_Historical]
    WHERE
        EventKey IN ('FQ2','FQ1','PQ','PQ1','PQ2','PQ3','PQ4','PQ5')    
        AND ClusterKey IN :release --('2025-N2','2024-N2')
    GROUP BY 
    UpdateYearWeekKey,
    GeographyKey, 
    ClusterKey, 
    EventKey,
    RIGHT(LEFT(ProductKey, 3), 2),
    RIGHT(UpdateDateKey,2), 
    ProductionSchedulerKey,
    ProductVariantsKey,
    ProductKey,
    RIGHT(ProductVariantsKey, 8),
    CASE WHEN EventKey='PQ1' THEN EventYearWeekKey ELSE 0 END,
    CASE WHEN EventKey='PQ1' THEN ExclusivityKey ELSE NULL END,
    CASE WHEN EventKey='PQ2' THEN EventYearWeekKey ELSE 0 END,
    CASE WHEN EventKey='PQ2' THEN ExclusivityKey ELSE NULL END,
    CASE WHEN EventKey='PQ3' THEN EventYearWeekKey ELSE 0 END,
    CASE WHEN EventKey='PQ3' THEN ExclusivityKey ELSE NULL END,
    CASE WHEN EventKey='PQ4' THEN EventYearWeekKey ELSE 0 END,
    CASE WHEN EventKey='PQ4' THEN ExclusivityKey ELSE NULL END,
    CASE WHEN EventKey='PQ5' THEN EventYearWeekKey ELSE 0 END,
    CASE WHEN EventKey='PQ5' THEN ExclusivityKey ELSE NULL END,
    CASE WHEN EventKey='PQ' THEN EventYearWeekKey ELSE 0 END,
    CASE WHEN EventKey='PQ' THEN ExclusivityKey ELSE NULL END,
    CASE WHEN EventKey='FQ1' THEN EventYearWeekKey ELSE 0 END,
    CASE WHEN EventKey='FQ1' THEN ExclusivityKey ELSE NULL END,
    CASE WHEN EventKey='FQ2' THEN EventYearWeekKey ELSE 0 END,
    CASE WHEN EventKey='FQ2' THEN ExclusivityKey ELSE NULL END
)


SELECT
        UpdateYearWeekKey,
        GeographyKey,
        ClusterKey,
        model,
        grid,
        WK,
        ProductionSchedulerKey,
        -- PQ1
        SUM(EventQty_PQ1) AS EventQty_PQ1,
        SUM(ChargedPreWeekN_PQ1) AS ChargedPreWeekN_PQ1,
        SUM(ChargedPostWeekN_PQ1) AS ChargedPostWeekN_PQ1,
        MAX(EventYearWeek_PQ1) AS EventYearWeek_PQ1,
        MAX(ExclusivityKey_PQ1) AS ExclusivityKey_PQ1,
        -- PQ2
        SUM(EventQty_PQ2) AS EventQty_PQ2,
        SUM(ChargedPreWeekN_PQ2) AS ChargedPreWeekN_PQ2,
        SUM(ChargedPostWeekN_PQ2) AS ChargedPostWeekN_PQ2,
        MAX(EventYearWeek_PQ2) AS EventYearWeek_PQ2,
        MAX(ExclusivityKey_PQ2) AS ExclusivityKey_PQ2,
        -- PQ3
        SUM(EventQty_PQ3) AS EventQty_PQ3,
        SUM(ChargedPreWeekN_PQ3) AS ChargedPreWeekN_PQ3,
        SUM(ChargedPostWeekN_PQ3) AS ChargedPostWeekN_PQ3,
        MAX(EventYearWeek_PQ3) AS EventYearWeek_PQ3,
        MAX(ExclusivityKey_PQ3) AS ExclusivityKey_PQ3,
        -- PQ4
        SUM(EventQty_PQ4) AS EventQty_PQ4,
        SUM(ChargedPreWeekN_PQ4) AS ChargedPreWeekN_PQ4,
        SUM(ChargedPostWeekN_PQ4) AS ChargedPostWeekN_PQ4,
        MAX(EventYearWeek_PQ4) AS EventYearWeek_PQ4,
        MAX(ExclusivityKey_PQ4) AS ExclusivityKey_PQ4,
        -- PQ5
        SUM(EventQty_PQ5) AS EventQty_PQ5,
        SUM(ChargedPreWeekN_PQ5) AS ChargedPreWeekN_PQ5,
        SUM(ChargedPostWeekN_PQ5) AS ChargedPostWeekN_PQ5,
        MAX(EventYearWeek_PQ5) AS EventYearWeek_PQ5,
        MAX(ExclusivityKey_PQ5) AS ExclusivityKey_PQ5,
        -- PQ
        SUM(EventQty_PQ) AS EventQty_PQ,
        SUM(ChargedPreWeekN_PQ) AS ChargedPreWeekN_PQ,
        SUM(ChargedPostWeekN_PQ) AS ChargedPostWeekN_PQ,
        MAX(EventYearWeek_PQ) AS EventYearWeek_PQ,
        MAX(ExclusivityKey_PQ) AS ExclusivityKey_PQ,
        --FQ1
        SUM(EventQty_FQ1) AS EventQty_FQ1,
        SUM(ChargedPreWeekN_FQ1) AS ChargedPreWeekN_FQ1,
        SUM(ChargedPostWeekN_FQ1) AS ChargedPostWeekN_FQ1,
        MAX(EventYearWeek_FQ1) AS EventYearWeek_FQ1,
        MAX(ExclusivityKey_FQ1) AS ExclusivityKey_FQ1,
        --FQ2
        SUM(EventQty_FQ2) AS EventQty_FQ2,
        SUM(ChargedPreWeekN_FQ2) AS ChargedPreWeekN_FQ2,
        SUM(ChargedPostWeekN_FQ2) AS ChargedPostWeekN_FQ2,
        MAX(ExclusivityKey_FQ2) AS ExclusivityKey_FQ2,
        MAX(EventYearWeek_FQ2) AS EventYearWeek_FQ2,
        --VERSATO LORDO (RIPETUTO UGUALE PER TUTTE
        MAX(TotalChargedQuantity) AS TotalChargedQuantity,
        SUM(TotalOrderedQuantity) AS TotalOrderedQuantity,
        SUM(PaperQuantity) AS PaperQuantity
FROM PRE_HISTORY
GROUP BY UpdateYearWeekKey,
        GeographyKey,
        ClusterKey,  
        model,
        grid,
        WK,
        ProductionSchedulerKey
        