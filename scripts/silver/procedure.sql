CREATE OR REPLACE PROCEDURE silver.load_silver()
LANGUAGE plpgsql
AS $$
DECLARE
	v_start TIMESTAMP := clock_timestamp()
BEGIN

	RAISE NOTICE '======================================================';
    RAISE NOTICE 'silver.load_silver() started at %', v_start;
    RAISE NOTICE '======================================================';

	-- ------------------------------------------------------------------
    -- TABLE 1 : exp_etcs_call
    -- Dedup key : (imsi, start_time)  — one call record per engine per moment
    -- Type fixes :
    --   stop_time            VARCHAR  → TIMESTAMP  (NULL when "Call running...")
    --   call_setup_duration  VARCHAR  → INTEGER ms
    --   transaction_duration VARCHAR  → INTEGER ms
    -- ------------------------------------------------------------------
	RAISE NOTICE '[1/7] Loading silver.exp_etcs_call ...';

	TRUNCATE TABLE silver.exp_etcs_call;
	
	INSERT INTO silver.exp_etcs_call (
		    start_time, stop_time,
	        call_setup_duration, transaction_duration,
	        etcs_baseline, system_version,
	        nid_engine, nid_operational,
	        imsi, msisdn, imei,
	        calling_number, called_number,
	        gsmr_connected, etcs_connected,
	        start_nid_c, start_nid_bg, stop_nid_c, stop_nid_bg, stop_d_lrbg,
	        root_failure, end_domain, protocol_layer, end_event, end_cause,
	        isdn_port_probe,
	        _batch_id, _source_file, _ingested_at, _silver_loaded_at, _row_num, _file_hash
	)
	SELECT
		start_time,
		NULLIF(TRIM(stop_time), 'Call running...')::TIMESTAMP,
		silver.parse_duration_ms(call_setup_duration),
		silver.parse_duration_ms(transaction_duration),
		etcs_baseline,
		system_version,
		nid_engine,
		nid_operational,
		imsi,
		msisdn,
		imei,
		calling_number,
		called_number,
		gsmr_connected,
		etcs_connected,
		start_nid_c,
		start_nid_bg,
		stop_nid_c,
		stop_nid_bg,
		stop_d_lrbg,
		root_failure, 
		end_domain,
		protocol_layer,
		end_event,
		end_cause,
		isdn_port_probe,
		_batch_id,
		_source_file,
		_ingested_at,
		NOW(),
		_row_num,
		_file_hash
	FROM (
		SELECT *, ROW_NUMBER() OVER (
			PARTITION BY imsi, start_time
			ORDER BY _ingested_at DESC
			) AS rn
		FROM bronze.exp_etcs_call
	)deduped
	WHERE rn = 1;

	RAISE NOTICE '[1/7] Done — % rows', (SELECT COUNT(*) FROM silver.exp_etcs_call);

    -- ------------------------------------------------------------------
    -- TABLE 2 : exp_transaction_tracing
    -- Dedup key : (imsi, start_time)
    -- Type fixes :
    --   stop_time            VARCHAR → TIMESTAMP  (NULL when "Call running...")
    --   call_setup_duration  VARCHAR → INTEGER ms
    --   establishment_delay  VARCHAR → INTEGER ms
    --   transaction_duration VARCHAR → INTEGER ms
    -- ------------------------------------------------------------------
	RAISE NOTICE '[2/7] Loading silver.exp_transaction_tracing ...';	
	
	TRUNCATE TABLE silver.exp_transaction_tracing;
	
	INSERT INTO silver.exp_transaction_tracing(
		    start_time, stop_time,
	        call_setup_duration, establishment_delay, transaction_duration,
	        start_lac, start_ci, stop_lac, stop_ci,
	        nid_engine, nid_operational,
	        tmsi, reallocated_tmsi,
	        imsi, msisdn, imei,
	        calling_number, called_number,
	        dest_route_address, functional_number, functional_number_ct,
	        direction, transaction_type, transaction_subtype, application_type,
	        gsmr_connected, priority, root_failure, protocol_layer,
	        end_event, end_cause, gb_ciphering_algo,
	        _batch_id, _source_file, _ingested_at, _silver_loaded_at, _row_num, _file_hash
	)
	SELECT
		start_time,
		NULLIF(stop_time, 'Call running...')::TIMESTAMP,
	    silver.parse_duration_ms(call_setup_duration),
	    silver.parse_duration_ms(establishment_delay),
	    silver.parse_duration_ms(transaction_duration),
		start_lac, 
		start_ci, 
		stop_lac, 
		stop_ci,
		nid_engine, 
		nid_operational,
		tmsi,
		reallocated_tmsi,
		imsi, 
		msisdn, 
		imei,
		calling_number, 
		called_number,
	    dest_route_address, 
		functional_number, 
		functional_number_ct,
	    direction, 
		transaction_type, 
		transaction_subtype, 
		application_type,
	    gsmr_connected, 
		priority, 
		root_failure, 
		protocol_layer,
	    end_event, 
		end_cause, 
		gb_ciphering_algo,
	    _batch_id, 
		_source_file, 
		_ingested_at, 
		NOW(), 
		_row_num, 
		_file_hash
	FROM(
		SELECT *,
		ROW_NUMBER() OVER (
			PARTITION BY imsi, start_time
			ORDER BY _ingested_at DESC
		) AS rn
		FROM bronze.exp_transaction_tracing
	) deduped
	WHERE rn = 1;

    RAISE NOTICE '[2/7] Done — % rows', (SELECT COUNT(*) FROM silver.exp_transaction_tracing);

    -- ------------------------------------------------------------------
    -- TABLE 3 : exp_vgcs_vbs_rec_tracing
    -- Dedup key : (functional_number, start_time)
    --   Note: imsi is NULL in group calls so we use functional_number instead
    -- Type fixes :
    --   establishment_delay     VARCHAR → INTEGER ms
    --   vgcs_duration           VARCHAR → INTEGER ms
    --   sccp/cell/dispatcher    "X / Y" → _numerator INT + _denominator INT
    --                                    + _rate DECIMAL(5,4) (NULL if denom=0)
    -- ------------------------------------------------------------------
    RAISE NOTICE '[3/7] Loading silver.exp_vgcs_vbs_rec_tracing ...';
	
	TRUNCATE TABLE silver.exp_vgcs_vbs_rec_tracing;
	
	INSERT INTO silver.exp_vgcs_vbs_rec_tracing(
	        start_time, stop_time,
	        application_type, transaction_type, transaction_subtype,
	        functional_number,
	        imsi, tmsi, msisdn,
	        gid, area, gcr, priority,
	        start_lac, start_ci,
	        establishment_delay,
	        sccp_success_numerator, sccp_success_denominator, sccp_success_rate,
	        cell_success_numerator, cell_success_denominator, cell_success_rate,
	        dispatcher_success_numerator, dispatcher_success_denominator, dispatcher_success_rate,
	        end_user,
	        vgcs_duration,
	        _batch_id, _source_file, _ingested_at, _silver_loaded_at, _row_num, _file_hash
	)
	SELECT
		start_time,
		stop_time,
		application_type, 
		transaction_type, 
		transaction_subtype,
	    functional_number,
	    imsi, 
		tmsi,
		msisdn,
	    gid, 
		area, 
		gcr, 
		priority,
	    start_lac, 
		start_ci,
		silver.parse_duration_ms(establishment_delay),
		(SELECT numerator FROM silver.parse_success_rate(sccp_success_rate)),
		(SELECT denominator FROM silver.parse_success_rate(sccp_success_rate)),
		(SELECT rats FROM silver.parse_success_rate(sccp_success_rate)),
		(SELECT numerator FROM silver.parse_success_rate(cell_success_rate)),
		(SELECT denominator FROM silver.parse_success_rate(cell_success_rate)),
		(SELECT rats FROM silver.parse_success_rate(cell_success_rate)),
		(SELECT numerator FROM silver.parse_success_rate(dispatcher_success_rate)),
		(SELECT denominator FROM silver.parse_success_rate(dispatcher_success_rate)),
		(SELECT rats FROM silver.parse_success_rate(dispatcher_success_rate)),
		end_user,
		silver.parse_duration_ms(vgcs_duration),
		_batch_id, 
		_source_file, 
		_ingested_at, 
		NOW(), 
		_row_num, 
		_file_hash
	FROM (
		SELECT *,
		ROW_NUMBER() OVER (
			PARTITION BY start_time, gid, functional_number
			ORDER BY _ingested_at DESC
		) AS rn
		FROM bronze.exp_vgcs_vbs_rec_tracing
	) deduped
	WHERE rn = 1;

    RAISE NOTICE '[3/7] Done — % rows', (SELECT COUNT(*) FROM silver.exp_vgcs_vbs_rec_tracing);

    -- ------------------------------------------------------------------
    -- TABLE 4 : exp_ho_tracing
    -- Dedup key : (imsi, ho_start_time)
    -- No type changes — ho_duration already DECIMAL(10,3) in Bronze
    -- ------------------------------------------------------------------
    RAISE NOTICE '[4/7] Loading silver.exp_ho_tracing ...';
	
	TRUNCATE TABLE silver.exp_ho_tracing;
	 
	INSERT INTO silver.exp_ho_tracing(
	    start_time, stop_time, ho_start_time, ho_duration,
		src_lac, src_ci, trg_lac, trg_ci,
	    imsi, msisdn, imei,
	 	ms_power, call_type, ho_type,
	    ho_end_event, ho_end_cause, ho_cause,
	    _batch_id, _source_file, _ingested_at, _silver_loaded_at, _row_num, _file_hash
	)
	SELECT 
		start_time, 
		stop_time, 
		ho_start_time, 
		ho_duration,
	    src_lac, 
		src_ci, 
		trg_lac, 
		trg_ci,
	    imsi, 
		msisdn, 
		imei,
	    ms_power, 
		call_type, 
		ho_type,
	    ho_end_event, 
		ho_end_cause, 
		ho_cause,
	    _batch_id, 
		_source_file, 
		_ingested_at, 
		NOW(),
		_row_num, 
		_file_hash
	FROM (
		SELECT *,
		ROW_NUMBER() OVER (
			PARTITION BY imsi, ho_start_time, src_lac, src_ci
			ORDER BY _ingested_at DESC
		) AS rn
		FROM bronze.exp_ho_tracing
	) deduped
	WHERE rn = 1;

    RAISE NOTICE '[4/7] Done — % rows', (SELECT COUNT(*) FROM silver.exp_ho_tracing);

    -- ------------------------------------------------------------------
    -- TABLE 5 : exp_subscriber_matrix
    -- Dedup key : (imsi)  — one current profile per subscriber
    -- No type changes
    -- ------------------------------------------------------------------
    RAISE NOTICE '[5/7] Loading silver.exp_subscriber_matrix ...';

	TRUNCATE TABLE silver.exp_subscriber_matrix;
	 
	INSERT INTO silver.exp_subscriber_matrix (
		imsi, last_imsi_time,
	    msisdn, last_msisdn_time,
	    nid_engine, last_nid_engine_time,
	    fn_ct3, last_fn_ct3_time,
	    fn_ct4, last_fn_ct4_time,
	    _batch_id, _source_file, _ingested_at, _silver_loaded_at, _row_num, _file_hash
	)
	SELECT
		imsi, last_imsi_time,
	    msisdn, last_msisdn_time,
	    nid_engine, last_nid_engine_time,
	    fn_ct3, last_fn_ct3_time,
		fn_ct4, last_fn_ct4_time,
		_batch_id, _source_file, _ingested_at, NOW(), _row_num, _file_hash
	FROM (
		SELECT *,
	        ROW_NUMBER() OVER (
	            PARTITION BY imsi
	            ORDER BY _ingested_at DESC
			) AS rn
	    FROM bronze.exp_subscriber_matrix
	) deduped
	WHERE rn = 1;

    RAISE NOTICE '[5/7] Done — % rows', (SELECT COUNT(*) FROM silver.exp_subscriber_matrix);
 
 
    -- ------------------------------------------------------------------
    -- TABLE 6 : exp_hdlc_frame_errors
    -- Dedup key : (nid_engine, frame_error_time, calling_number, direction)
    -- No type changes
    -- ------------------------------------------------------------------
    RAISE NOTICE '[6/7] Loading silver.exp_hdlc_frame_errors ...';
	
	TRUNCATE TABLE silver.exp_hdlc_frame_errors;
	 
	INSERT INTO silver.exp_hdlc_frame_errors (
		start_time, stop_time, frame_error_time,
		isdn_port_probe,
		nid_engine, nid_operational,
		calling_number, called_number,
		last_nid_c, last_nid_bg,
		m_level, m_mode, direction,
		frame_error, frame_error_retransmission_count,
		_batch_id, _source_file, _ingested_at, _silver_loaded_at, _row_num, _file_hash
	    )
	SELECT
		start_time, stop_time, frame_error_time,
		isdn_port_probe,
		nid_engine, nid_operational,
		calling_number, called_number,
		last_nid_c, last_nid_bg,
		m_level, m_mode, direction,
		frame_error, frame_error_retransmission_count,
		_batch_id, _source_file, _ingested_at, NOW(), _row_num, _file_hash
	FROM (
		SELECT *,
			ROW_NUMBER() OVER (
				PARTITION BY nid_engine, frame_error_time,
							 calling_number, direction
				ORDER BY _ingested_at DESC
			) AS rn
		FROM bronze.exp_hdlc_frame_errors
	) deduped
	WHERE rn = 1;

    RAISE NOTICE '[6/7] Done — % rows', (SELECT COUNT(*) FROM silver.exp_hdlc_frame_errors);
 
 
    -- ------------------------------------------------------------------
    -- TABLE 7 : sp_ertms_disconnects
    -- Dedup key : (date, heure, imei)
    -- No type changes
    -- ------------------------------------------------------------------
    RAISE NOTICE '[7/7] Loading silver.sp_ertms_disconnects ...';
	
	TRUNCATE TABLE silver.sp_ertms_disconnects;
	
	INSERT INTO silver.sp_ertms_disconnects (
		nombre_ordre, derniere_7_jours, date, heure,
		numero_train, rame, motrice_cab, mrm, imei,
		sens, km, intervalle, niveau_etcs, evenement,
		cause_racine, analyse_smmrgv, sous_systeme_mis_en_cause,
		action, execution_ho,
		rxqual, rxlev, voisinage_10sec,
		com_dte_dce, ecart, retransmission_trames_hdlc_t70,
		alarmes_bts, traite_par,
		cire, ciers, acquittement_auc, bug_rbc, liens_pai,
		pilote, echeance,
		_batch_id, _source_file, _ingested_at, _silver_loaded_at, _row_num, _file_hash
	)
	SELECT
		nombre_ordre, derniere_7_jours, date, heure,
		numero_train, rame, motrice_cab, 
		CASE
	    WHEN TRIM(mrm) ~ '^\d+$'
	    THEN TRIM(mrm)::INTEGER
	    ELSE NULL
		END, 
		imei,
		sens, 
		CASE
	    WHEN REPLACE(TRIM(km), ',', '.') ~ '^\d+(\.\d+)?$'
	    THEN REPLACE(TRIM(km), ',', '.')::DECIMAL(8,3)
	    ELSE NULL
		END,
		intervalle, niveau_etcs, evenement,
		cause_racine, analyse_smmrgv, sous_systeme_mis_en_cause,
		action, execution_ho,
		rxqual, rxlev, voisinage_10sec,
		com_dte_dce, ecart, retransmission_trames_hdlc_t70,
		alarmes_bts, traite_par,
		cire, ciers, acquittement_auc, bug_rbc, liens_pai,
		pilote, echeance,
		_batch_id, _source_file, _ingested_at, NOW(), _row_num, _file_hash
	FROM (
		SELECT *,
			ROW_NUMBER() OVER (
				PARTITION BY date, heure, imei
				ORDER BY _ingested_at DESC
			) AS rn
		FROM bronze.sp_ertms_disconnects
	) deduped
	WHERE rn = 1;

    RAISE NOTICE '[7/7] Done — % rows', (SELECT COUNT(*) FROM silver.sp_ertms_disconnects);
 
    RAISE NOTICE '======================================================';
    RAISE NOTICE 'silver.load_silver() completed in % seconds',
                 EXTRACT(EPOCH FROM clock_timestamp() - v_start);
    RAISE NOTICE '======================================================';
 
END;
$$;
