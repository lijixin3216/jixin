USE [AN_SMART]
GO

/****** Object:  StoredProcedure [dbo].[USP_UPD_SMART_CLUSTER_Q_VALUE]    Script Date: 09/08/2014 08:39:23 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO





ALTER PROCEDURE [dbo].[USP_UPD_SMART_CLUSTER_Q_VALUE] (
	@current_cluster_set INT = NULL
	,@lookback_window INT = 7
	,@minimum_placement_volume INT = 2800
	)
AS
-- =============================================
-- Author:		  Kaushik Sinha 
-- Name:          USP_UPD_SMART_CLUSTER_Q_VALUE
-- Create date:   12/15/2011
-- Description:	Initial Version of the script.
-- Modifications: 								
-- 12/15/2011			Sanjay Dhakhwa				Changed the script to stored procedure.
-- 01/12/2012			Sanjay Dhakhwa				Replaced daily_impression_frequency with daily_flightmedia_cluster_metrics
--												         with script from J. Blatz.
-- 01/17/2012			Sanjay Dhakhwa				Changed from AN_STAGE to AN_RESEARCH for automating loads from hive to SQL.		
-- 03/28/2012			Sanjay Dhakhwa				Added filter on fmid (-1,-2,-6) as per John Blatz.										
-- 10/24/2012			Craig Jacobik				Added filter on fmid (-7) in addition to the existing fmid exclusions
--												         and removed the CTR and Completed calculations per Carrie's request
-- 12/14/2012			Craig Jacobik				Removed all mention of clicks/completes
-- 06/26/2013			Chun-Sheng Chen				Using active flag to identify current clusters; added condition to not 
--													update q for placements being delta clustered within lookback_window
-- 09/27/2013			Kaushik Sinha				Update procedure to use OPT_RPT..hourly_flightmedia_cluster_metrics
--													instead of AN_RESEARCH..daily_flightmedia_cluster_metrics
-- 10/23/2013			Kaushik S.					Normalize the q values on any pids where we detect that sum of q values 
--													is not within a particular threshold.
-- 11/15/2013			Kaushik S.					Per Carrie, exclude Display pids from q value updates since OPT_RPT..hourly_flightmedia_cluster_metrics
--													does not have timely updates for display imp/request data.	
-- 01/14/2014			Kaushik S.					Updating lookback window to be 7 days; Volume threshold to be 2800; 												
--													This is to help with q estimates for dayparted clusters.
-- 04/11/2014			Kaushik S.					Update so that for clusters without any requests over the lookback window 
--													(but with their pid having more than the placement minimum over lookback) 
--													get their q values set to zero.
--													Per Mark, for upfront pids start using total_served column in OPT_RPT..hourly_flightmedia_cluster_metrics
--													so that we can account for dropoff.
-- 06/04/2014			Kaushik S.					Remove blocking of Display pids from q updates now that we are getting timely data on Display pids
-- 07/25/2014			James V.					1. Record inputs that went into q value calculation into a new table. 
--													2. For pids that have been delta clustered in the past week, once these pids are over the q freeze period,
--													adjust the lookback window to be from the time since delta clustering until the current lookback window
--													is reached. 	
--													3. Fix off by one in the look back window for computing q, so lookback window being used
--													before this was 8 days. This is set to 7 now.	
-- 09/08/2014			Kaushik S.					Shorten q freeze after Delta Clustering to 3	
-- 10/13/2014           Eric H.                     Pull additional pids to ignore from new table AN_SMART..q_estimation_pids_to_ignore, when
--                                                  the current procedure run time falls within the corresponding date range, and add them to
--                                                  #blocked_pids.								
-- =============================================
BEGIN
	SET NOCOUNT ON;

	DECLARE @ErrMsg NVARCHAR(4000)
		,@ErrSeverity INT
		,@execTime DATETIME = GETDATE ()
		,@routine_name VARCHAR(128) -- for logging and exception handling
		-- Insert all your SP scripts inside the TRY block

	SELECT @routine_name = OBJECT_NAME(@@PROCID)

	BEGIN TRY
			
			--Determine days that have elapsed for pids that 
			--were delta clustered in the past week
			SELECT DISTINCT c.placement_id
			, DATEDIFF(dd, r.run_date, GETDATE()) AS days_since_clustered 
			INTO #pid_clustering_dates 
			FROM AN_SMART..delta_cluster_run_cluster_summary cs (NOLOCK)
            INNER JOIN AN_SMART..delta_cluster_run r (NOLOCK)
            ON cs.delta_cluster_run_id=r.delta_clustering_run_id
            INNER JOIN AN_SMART..cluster c (NOLOCK)
            ON cs.cluster_id=c.cluster_id
            WHERE r.run_date > DATEADD(dd,-1 * 7, GETDATE())
            ORDER BY c.placement_id;
            
            --For active pids determine the lookback window to use
            --for q estimates. In particular, set lookback window
            --for pids delta clustered in the past week to
            --be the number of days since they were delta clustered.
            SELECT p.placement_id
            , (CASE WHEN pcd.days_since_clustered IS NULL THEN @lookback_window 
				ELSE pcd.days_since_clustered END) AS lookback_window 
            INTO #lookback_windows
            FROM AN_MAIN..placement p 
            LEFT JOIN #pid_clustering_dates pcd
            ON p.placement_id = pcd.placement_id
            WHERE p.active = 1
            ORDER BY lookback_window;
            
			--Get cluster level totals
			SELECT p.passback_allowed
				,c.placement_id
				, c.cluster_id
				,SUM(CASE WHEN p.passback_allowed = 0 THEN CAST(f.total_served AS BIGINT) ELSE CAST(f.requests AS BIGINT) END) AS cluster_requests
				, lw.lookback_window
			INTO #cluster_impreq_totals
			FROM OPT_RPT..hourly_flightmedia_cluster_metrics AS f WITH (NOLOCK)
			INNER JOIN AN_SMART..cluster AS c (NOLOCK) 
			ON c.cluster_id = f.cluster_id AND c.active=1
			INNER JOIN AN_MAIN..placement p (NOLOCK)
			ON p.placement_id = c.placement_id 
			INNER JOIN #lookback_windows lw
			ON lw.placement_id = p.placement_id
			WHERE 
				f.keydate > DATEADD(dd, - 1 * lw.lookback_window, GETDATE())--@lookback_window
				AND f.flight_media_id NOT IN (
					- 1
					,- 2
					,- 6
					,- 7
					)
			GROUP BY p.passback_allowed, c.placement_id, c.cluster_id, lw.lookback_window
			
			
			--clean up table on table that keeps cluster level inputs
			--remove data older than past 7 days
			DELETE FROM AN_SMART..q_estimation_inputs
			WHERE keydate < CONVERT (date, DATEADD(dd,-1 * 7, GETDATE()));			
			
			--persist cluster level totals into a table
			INSERT INTO AN_SMART..q_estimation_inputs
			SELECT CONVERT (date, GETDATE()) as keydate, 
			passback_allowed, placement_id, cluster_id, cluster_requests, lookback_window, GETDATE() as createdon
			FROM #cluster_impreq_totals;
			
		  -- Compute placement totals for those pids and compute flag to see if they meet minimum request threshold
		  SELECT placement_id, SUM(cluster_requests) as pid_requests,
		  CASE WHEN SUM(cluster_requests) > @minimum_placement_volume THEN 1 ELSE 0 END AS meets_pid_volume_threshold --@minimum_placement_volume
		  INTO #pid_impreq_totals --drop table #pid_impreq_totals
		  FROM #cluster_impreq_totals
		  GROUP BY placement_id

		  --Compute first cut to the q values
		  --some of these will be null (these we will coalesce away at the end)
		  SELECT c.active, c.placement_id, c.cluster_id, 
			  COALESCE(pcit.cluster_requests, 0) AS logged_cluster_total_requests,
			  pit.pid_requests AS logged_placement_total_requests,
			  CASE WHEN pit.pid_requests = 0 THEN NULL ELSE (1.0*COALESCE(pcit.cluster_requests, 0))/(1.0*pit.pid_requests) END AS logged_cluster_q,
			  pit.meets_pid_volume_threshold
		  INTO #first_cut_q --drop table #first_cut_q
		  FROM AN_SMART..cluster c (NOLOCK)
		  LEFT JOIN #cluster_impreq_totals pcit
			ON pcit.cluster_id = c.cluster_id 
		  LEFT JOIN #pid_impreq_totals pit
			ON pit.placement_id = c.placement_id
		  WHERE c.active = 1
		  ORDER BY c.active, c.placement_id, c.ranking
		    
		--Create a list of placements for which we do not want to update q's
		--Block out placements where delta_clustering ocurred within @lookback_window
		DECLARE @deltaClusteringLookbackWindow INT = 3
		
		SELECT DISTINCT placement_id 
		INTO #blocked_pids 
		FROM #pid_clustering_dates
		WHERE days_since_clustered < @deltaClusteringLookbackWindow;
		--End DeltaClustering block
		
		--Block out pids that did not receive ANY requests
		--or below the threshold volume
		--so that their last known q values are preserved
		INSERT INTO #blocked_pids
		SELECT t.placement_id FROM 
		(
			SELECT DISTINCT placement_id
			FROM #first_cut_q
			WHERE meets_pid_volume_threshold IS NULL OR meets_pid_volume_threshold = 0
		) t
		LEFT JOIN #blocked_pids bp
		ON t.placement_id = bp.placement_id AND bp.placement_id IS NULL
		-- End blocking out pids w/o any requests
		
		--Grab additional pids to ignore, consistent with current date
		SELECT DISTINCT placement_id
		INTO #pids_to_ignore
		FROM AN_SMART..q_estimation_pids_to_ignore
		WHERE @execTime BETWEEN ignore_from_date and ignore_to_date
		--Block them
		MERGE #blocked_pids AS target
		USING #pids_to_ignore AS source
		ON (target.placement_id = source.placement_id)
		WHEN NOT MATCHED BY TARGET THEN
		    INSERT (placement_id)
		    VALUES (source.placement_id)
		
		--Create a q value table
		--NOTE: In this step we will remove any entries for the blocked pids from #blocked_pids
		SELECT nqv.placement_id, nqv.cluster_id
		, nqv.logged_placement_total_requests, nqv.logged_cluster_total_requests,
		nqv.logged_cluster_q
		INTO #q_values_to_update
		FROM #first_cut_q nqv
		LEFT JOIN #blocked_pids bp
		ON bp.placement_id = nqv.placement_id
		WHERE bp.placement_id IS NULL
		
		--Create the final_q_value table
		--Keep track of the existing q values (may use for debugging)
		SELECT c.cluster_type_id, c.cluster_id, c.placement_id, c.active, c.q_value as current_q, COALESCE(fqv.logged_cluster_q, 0.0) as candidate_q_value
		INTO #final_q_values --drop table #final_q_values
		FROM AN_SMART..cluster c (NOLOCK)
		INNER JOIN #q_values_to_update fqv
		ON c.cluster_id = fqv.cluster_id AND c.active=1 
		
		----Some debugging error checks
		--SELECT * from #final_q_values 
		--where candidate_q_value = 0 and current_q > 0
		--order by placement_id, cluster_type_id
		
		--SELECT placement_id, SUM(candidate_q_value) from #final_q_values GROUP BY placement_id ORDER BY SUM(candidate_q_value) DESC
		
		--SELECT * from #final_q_values WHERE placement_id=4722
		--SELECT * from #q_values_to_update WHERE placement_id=4722
		
		----These should be in similar proportions
		--SELECT COUNT(*) from #final_q_values where candidate_q_value > 0 AND active=1
		--SELECT COUNT(*) from #final_q_values where current_q > 0 AND active=1
		--SELECT COUNT(*) from AN_SMART..cluster where q_value > 0 AND active=1 AND placement_id in (SELECT DISTINCT placement_id FROM #final_q_values)
		
		--select cluster_id, current_q, candidate_q_value, abs(current_q-candidate_q_value)
		--from #final_q_values
		--order by abs(current_q-candidate_q_value) desc
		--End debugging 
		
		--Now update the q values
		UPDATE c
		SET c.q_value = fqv.candidate_q_value, 
			c.q_mprime = fqv.candidate_q_value,
			change_source = 'Old Q update I'
		FROM AN_SMART..cluster c
		INNER JOIN #final_q_values fqv
		ON c.cluster_id = fqv.cluster_id AND c.active=1
		--End q value update
		
		--q update is DONE, now do some bookkeeping      
		--Normalize q values for pids
		SELECT c.placement_id
		, p.placement_status_id
		, SUM(c.q_value) as sum_q
		INTO #pids_with_q_neq_1
		FROM AN_SMART..cluster c (NOLOCK)
		INNER JOIN AN_MAIN..placement p (NOLOCK)
		ON c.placement_id = p.placement_id 
			AND p.placement_status_id IN (1,2) --approved or likely
			AND c.active = 1 
		GROUP BY c.placement_id, p.placement_status_id
		HAVING (SUM(c.q_value) < 0.99 OR SUM(c.q_value) > 1.01)
		ORDER BY SUM(c.q_value) DESC
		
		SELECT c.placement_id
		,c.cluster_id
		,c.q_value as old_q_value
		,(c.q_value/(CASE WHEN t.sum_q <= 0 THEN 1 ELSE t.sum_q END)) AS new_q_value 
		INTO #updated_q_values
		FROM AN_SMART..cluster c
		INNER JOIN #pids_with_q_neq_1 t
		ON t.placement_id = c.placement_id AND c.active = 1
		
		UPDATE c
		SET c.q_value = t.new_q_value, 
			c.q_mprime = t.new_q_value,
			change_source = 'Old Q update II'
		FROM AN_SMART..cluster c
		INNER JOIN #updated_q_values t
		ON t.placement_id = c.placement_id AND t.cluster_id = c.cluster_id
		-- End of normalize q

		  --============================================================================== 
          --============================================================================== 
		  --New Hourly Q update 
			SELECT placement_id 
					,cluster_id 
					,Sum(cluster_requests)                                 AS cluster_req 
					,Sum(placement_requests)                               AS pid_req 
					,cast(Sum(cluster_requests) as decimal) / Sum(placement_requests) AS q_value 
			INTO   #hourly_q 
			FROM   an_smart..hourly_cluster_q_value(nolock) 
			GROUP  BY placement_id 
					,cluster_id 
			HAVING Sum(placement_requests) > 400

			SELECT placement_id 
					,Sum(q_value) AS sum_q 
			INTO   #pid_sum_q
			FROM   #hourly_q 
			GROUP  BY placement_id 
			HAVING Sum(q_value) > 0 

			update c
			SET    c.q_value = coalesce(cast(q.q_value as numeric(10,8)), 0)/p.sum_q
					,c.q_mprime =  coalesce(cast(q.q_value as numeric(10,8)), 0)/p.sum_q
					,change_source = 'Hourly Q update' 
			from an_smart..cluster(nolock) c
			JOIN #pid_sum_q p ON p.placement_id = c.placement_id 
			left join #hourly_q q on q.cluster_id = c.cluster_id
			where c.active = 1 

      --=============================================================================== 
		
	END TRY

	-- Handle errors in the CATCH block below
	BEGIN CATCH
		---------------Whoops, there was an error
		EXEC LOGGING_DB.dbo.USP_LOGGING @routine_name
			,@execTime

		SELECT @ErrMsg = ERROR_MESSAGE()
			,@ErrSeverity = ERROR_SEVERITY()

		RAISERROR (
				@ErrMsg
				,@ErrSeverity
				,1
				)
		WITH NOWAIT
	END CATCH

	EXEC LOGGING_DB.dbo.USP_LOGGING @routine_name
		,@execTime
END










GO

