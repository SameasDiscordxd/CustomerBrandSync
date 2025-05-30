USE [Venom]
GO

ALTER PROCEDURE [dbo].[GetNewCustomersForGoogleAdsWithBrandInfo] 
    @FullUpload BIT = 0 -- Only parameter needed for the script
AS
/*
=============================================
OPTIMIZED VERSION FOR NODE.JS SCRIPT
Description: Returns customer data with segment flags efficiently
             Removes @SegmentFilter parameter for better performance
=============================================
*/
BEGIN
    SET NOCOUNT ON;
    DECLARE @LastUploadDate DATETIME
    DECLARE @RowsProcessed INT = 0
    
    -- Get the last upload date
    SELECT @LastUploadDate = MAX(LastUploadDate)
    FROM dbo.GoogleAdsUploadTracking
    WHERE SuccessFlag = 1
    
    -- If last upload date is NULL, set a default value (30 days ago)
    IF @LastUploadDate IS NULL
        SET @LastUploadDate = DATEADD(day, -30, GETDATE());

    -- Simplified approach: Get base customer data first, then add segment flags
    WITH BaseCustomers AS (
        SELECT DISTINCT
            c.CustomerNumber,
            c.FirstName,
            c.LastName,
            c.ContactGUID,
            ih.CustomerEmail,
            ih.CustomerPhoneNumber,
            ih.CustomerZipCode,
            ih.CustomerState AS StateCode,
            COALESCE(sb.Name, 'default') AS BrandId,
            c.AddDate,
            c.ChangeDate
        FROM dbo.Customer AS c
        INNER JOIN dbo.InvoiceHeader AS ih ON c.Id = ih.CustomerId
        LEFT JOIN dbo.Store AS s ON ih.StoreId = s.Id
        LEFT JOIN dbo.StoreBrand AS sb ON s.StoreBrandId = sb.Id
        WHERE 
            COALESCE(sb.Name, 'default') IN ('Big Brand Tire', 'American Tire Depot', 'Tire World')
            AND ((ih.CustomerEmail IS NOT NULL AND ih.CustomerEmail <> '')
                OR (ih.CustomerPhoneNumber IS NOT NULL AND ih.CustomerPhoneNumber <> ''))
            AND ih.StatusId = 3
            AND (
                @FullUpload = 1 -- If full upload, ignore date filtering
                OR c.AddDate > @LastUploadDate
                OR c.ChangeDate > @LastUploadDate
            )
    ),
    
    -- Get tire purchase flags
    TireCustomers AS (
        SELECT DISTINCT bc.CustomerNumber, bc.BrandId
        FROM BaseCustomers bc
        INNER JOIN dbo.InvoiceHeader ih ON bc.CustomerEmail = ih.CustomerEmail OR bc.CustomerPhoneNumber = ih.CustomerPhoneNumber
        INNER JOIN dbo.InvoiceDetail id ON ih.Id = id.InvoiceHeaderId
        INNER JOIN dbo.InventoryItem ii ON id.ItemId = ii.ItemId
        WHERE ii.PartTypeId = 13688
          AND id.Active = 1 AND id.Approved = 1
    ),
    
    -- Get service purchase flags
    ServiceCustomers AS (
        SELECT DISTINCT bc.CustomerNumber, bc.BrandId
        FROM BaseCustomers bc
        INNER JOIN dbo.InvoiceHeader ih ON bc.CustomerEmail = ih.CustomerEmail OR bc.CustomerPhoneNumber = ih.CustomerPhoneNumber
        INNER JOIN dbo.InvoiceDetail id ON ih.Id = id.InvoiceHeaderId
        WHERE (LEFT(id.PartNumber, 3) = 'LAB' OR id.ItemId NOT IN (SELECT ItemId FROM dbo.InventoryItem WHERE PartTypeId = 13688))
          AND id.Active = 1 AND id.Approved = 1
    ),
    
    -- Get visit counts and last purchase dates
    CustomerActivity AS (
        SELECT 
            bc.CustomerNumber,
            bc.BrandId,
            COUNT(DISTINCT ih.InvoicedDate) AS TotalVisits,
            MAX(ih.InvoicedDate) AS LastPurchaseDate,
            DATEDIFF(MONTH, MAX(ih.InvoicedDate), GETDATE()) AS MonthsSinceLastPurchase
        FROM BaseCustomers bc
        INNER JOIN dbo.InvoiceHeader ih ON bc.CustomerEmail = ih.CustomerEmail OR bc.CustomerPhoneNumber = ih.CustomerPhoneNumber
        WHERE ih.StatusId = 3
        GROUP BY bc.CustomerNumber, bc.BrandId
    ),
    
    -- Non-customers from MailChimp
    NonCustomers AS (
        SELECT DISTINCT
            'Non-Customer' AS CustomerNumber,
            mc.FirstName,
            mc.LastName,
            NULL AS ContactGUID,
            mc.Email AS CustomerEmail,
            NULL AS CustomerPhoneNumber,
            NULL AS CustomerZipCode,
            NULL AS StateCode,
            COALESCE(mc.PreferredBrand, 'default') AS BrandId,
            NULL AS LastPurchaseDate,
            0 AS TotalVisits,
            NULL AS MonthsSinceLastPurchase,
            1 AS IsNonCustomer,
            0 AS IsTireCustomer,
            0 AS IsServiceCustomer,
            0 AS IsRepeatCustomer,
            0 AS IsLapsedCustomer
        FROM dbo.MailChimp mc
        WHERE mc.MC_Status = 'subscribed' 
          AND (mc.VenomCustomerId = 0 OR mc.VenomCustomerId IS NULL)
          AND mc.PreferredBrand IN ('Big Brand Tire', 'American Tire Depot', 'Tire World')
    )

    -- Final result set with all segment flags
    SELECT 
        bc.BrandId,
        bc.CustomerNumber,
        bc.FirstName,
        bc.LastName,
        bc.ContactGUID,
        bc.CustomerEmail,
        bc.CustomerPhoneNumber,
        bc.CustomerZipCode,
        bc.StateCode,
        ca.LastPurchaseDate,
        ca.TotalVisits,
        ca.MonthsSinceLastPurchase,
        
        -- Segment flags
        0 AS IsNonCustomer,
        CASE WHEN tc.CustomerNumber IS NOT NULL THEN 1 ELSE 0 END AS IsTireCustomer,
        CASE WHEN sc.CustomerNumber IS NOT NULL THEN 1 ELSE 0 END AS IsServiceCustomer,
        CASE WHEN ca.TotalVisits > 1 THEN 1 ELSE 0 END AS IsRepeatCustomer,
        CASE WHEN ca.MonthsSinceLastPurchase >= 15 THEN 1 ELSE 0 END AS IsLapsedCustomer
        
    FROM BaseCustomers bc
    LEFT JOIN TireCustomers tc ON bc.CustomerNumber = tc.CustomerNumber AND bc.BrandId = tc.BrandId
    LEFT JOIN ServiceCustomers sc ON bc.CustomerNumber = sc.CustomerNumber AND bc.BrandId = sc.BrandId
    LEFT JOIN CustomerActivity ca ON bc.CustomerNumber = ca.CustomerNumber AND bc.BrandId = ca.BrandId

    UNION ALL

    -- Add non-customers
    SELECT 
        BrandId, CustomerNumber, FirstName, LastName, ContactGUID,
        CustomerEmail, CustomerPhoneNumber, CustomerZipCode, StateCode,
        LastPurchaseDate, TotalVisits, MonthsSinceLastPurchase,
        IsNonCustomer, IsTireCustomer, IsServiceCustomer, IsRepeatCustomer, IsLapsedCustomer
    FROM NonCustomers

    ORDER BY BrandId, CustomerNumber;

    -- Simple tracking
    SET @RowsProcessed = @@ROWCOUNT;
    
    INSERT INTO dbo.GoogleAdsUploadTracking (
        LastUploadDate, 
        UploadDescription, 
        RowsProcessed, 
        SuccessFlag,
        ActualUploadedCount,
        BrandName,
        BrandListId,
        BrandRowsProcessed
    )
    VALUES (
        GETDATE(),
        CASE 
            WHEN @FullUpload = 1 THEN 'Full Multi-Segment Upload - Optimized'
            ELSE 'Delta Multi-Segment Upload - Optimized'
        END,
        @RowsProcessed,
        1,
        @RowsProcessed,
        'All Brands - Optimized Processing',
        NULL,
        @RowsProcessed
    );

END