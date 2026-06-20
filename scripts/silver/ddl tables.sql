/*
this code should run before the ingestion, it creates expandium and sharepoint tables and adding meta data for tracking, and ingestion track tables (ingest_files and ingest_runs) that helps on
tracking the runs with it's primary key, time of start and end, status, and how many rows, and tracking files with there primary key, the id of the run that means
which runs they were in, source syteme (like expandium or sharepoint), processed at, row counts and it status
*/

/*
Converts strings like "43min 16s 695ms" to 2596695 ms total milliseconds
and returns NULL for non-duration values ("Call running...", NULL, empty)
*/
CREATE OR REPLACE FUNCTION silver.parse_duration_ms(val TEXT)
RETURNS INTEGER
LANGUAGE plpgsql
IMMUTABLE AS $$
BEGIN
	IF val IS NULL OR val !~ '\d' THEN
	RETURN NULL;
	END IF;
	RETURN (
		COALESCE((regexp_match(val, '(\d+)min'))[1]::INTEGER,0) * 60000
		+ COALESCE((regexp_match(val, '(\d+)s(?![a-z])'))[1]::INTEGER,0) * 1000
		+ COALESCE((regexp_match(val, '(\d+)ms'))[1]::INTEGER,0)
	);
END;
$$;

/*
this function takes the values "3 / 4" and puts them in a temporary 
table with columns numerator, denominator and rate which is numintor/denominator
*/
CREATE OR REPLACE FUNCTION silver.parse_success_rate(val TEXT)
RETURNS TABLE(numerator INTEGER, denominator INTEGER, rats DECIMAL(5,4))
LANGUAGE plpgsql
IMMUTABLE AS $$
BEGIN
	IF val IS NULL OR val NOT LIKE '% / %' THEN
		RETURN QUERY SELECT NULL::INTEGER, NULL::INTEGER, NULL::DECIMAL(5,4);
		RETURN;
	END IF;

	RETURN QUERY
	SELECT 
		NULLIF(SPLIT_PART(val, ' / ', 1), '')::INTEGER,
		NULLIF(SPLIT_PART(val, ' / ', 2), '')::INTEGER,
		CASE
			WHEN NULLIF(SPLIT_PART(val, ' / ',2),'')::INTEGER = 0 THEN NULL
			ELSE ROUND(
				NULLIF(SPLIT_PART(val, ' / ', 1), '')::DECIMAL / NULLIF(SPLIT_PART(val, ' / ', 2), '')::DECIMAL,
				4
			)
		END;
END;
$$;

DROP TABLE IF EXISTS silver.ingest_runs CASCADE;
CREATE TABLE silver.ingest_runs(
	batch_id VARCHAR(100) PRIMARY KEY,
	start_time TIMESTAMP WITHOUT TIME ZONE,
	end_time TIMESTAMP WITHOUT TIME ZONE,
	status VARCHAR(20),
	total_files INT,
	total_rows INT,
	error_message TEXT
);

DROP TABLE IF EXISTS silver.exp_etcs_call CASCADE;
CREATE TABLE silver.exp_etcs_call(
    start_time TIMESTAMP,
    stop_time TIMESTAMP,
    call_setup_duration INTEGER,
    transaction_duration INTEGER,                    
    etcs_baseline SMALLINT,
    system_version DECIMAL(3,1),
    nid_engine INTEGER,
    nid_operational INTEGER,
    imsi BIGINT,
    msisdn BIGINT,                         
    imei BIGINT,
    calling_number BIGINT,
    called_number INTEGER,
    gsmr_connected VARCHAR(50),
    etcs_connected VARCHAR(50),
    start_nid_c INTEGER,
    start_nid_bg INTEGER,
    stop_nid_c INTEGER,
    stop_nid_bg INTEGER,
    stop_d_lrbg INTEGER,
    root_failure VARCHAR(50),
    end_domain VARCHAR(50),
    protocol_layer VARCHAR(50),
    end_event VARCHAR(255),
    end_cause VARCHAR(255),
    isdn_port_probe VARCHAR(255),
	_batch_id TEXT,  
	_source_file TEXT,
    _ingested_at TIMESTAMP,
	_silver_loaded_at TIMESTAMP DEFAULT NOW(),
    _row_num INT,
    _file_hash TEXT
);

DROP TABLE IF EXISTS silver.exp_hdlc_frame_errors CASCADE;
CREATE TABLE silver.exp_hdlc_frame_errors(
    start_time TIMESTAMP,
    stop_time TIMESTAMP,
    frame_error_time TIMESTAMP,
    isdn_port_probe VARCHAR(255),
    nid_engine INT,
    nid_operational INT,
    calling_number BIGINT,
    called_number INT,
    last_nid_c INT,
    last_nid_bg INT,
    m_level VARCHAR(20),
    m_mode VARCHAR(50),
    direction VARCHAR(50),
    frame_error VARCHAR(100),
    frame_error_retransmission_count INT,
	_batch_id TEXT,  
	_source_file TEXT,
    _ingested_at TIMESTAMP,
	_silver_loaded_at TIMESTAMP DEFAULT NOW(),
    _row_num INT,
    _file_hash TEXT
);

DROP TABLE IF EXISTS silver.exp_subscriber_matrix CASCADE;
CREATE TABLE silver.exp_subscriber_matrix(
    imsi BIGINT,
    last_imsi_time TIMESTAMP,
    msisdn BIGINT,
    last_msisdn_time TIMESTAMP,
    nid_engine INT,
    last_nid_engine_time TIMESTAMP,
    fn_ct3 BIGINT,
    last_fn_ct3_time TIMESTAMP,
    fn_ct4 BIGINT,
    last_fn_ct4_time TIMESTAMP,
	_batch_id TEXT,  
	_source_file TEXT,
    _ingested_at TIMESTAMP,
	_silver_loaded_at TIMESTAMP DEFAULT NOW(),
    _row_num INT,
    _file_hash TEXT
);

DROP TABLE IF EXISTS silver.exp_ho_tracing CASCADE;
CREATE TABLE silver.exp_ho_tracing(
    start_time TIMESTAMP,
    stop_time TIMESTAMP,
    ho_start_time TIMESTAMP,
    ho_duration DECIMAL(10,3),
    src_lac INT,
    src_ci INT,
    trg_lac INT,
    trg_ci INT,
    imsi BIGINT,
    msisdn BIGINT,
    imei BIGINT,
    ms_power VARCHAR(50),
    call_type VARCHAR(50),
    ho_type VARCHAR(50),
    ho_end_event VARCHAR(100),
    ho_end_cause VARCHAR(100),
    ho_cause VARCHAR(100),
	_batch_id TEXT,  
	_source_file TEXT,
    _ingested_at TIMESTAMP,
	_silver_loaded_at TIMESTAMP DEFAULT NOW(),
    _row_num INT,
    _file_hash TEXT
);

DROP TABLE IF EXISTS silver.exp_vgcs_vbs_rec_tracing CASCADE;
CREATE TABLE silver.exp_vgcs_vbs_rec_tracing(
    start_time TIMESTAMP,
    stop_time TIMESTAMP,
    application_type VARCHAR(100),
    transaction_type VARCHAR(50),
    transaction_subtype VARCHAR(50),
    functional_number BIGINT,
    imsi BIGINT,
    tmsi VARCHAR(20),
    msisdn BIGINT,
    gid INTEGER,
    area INTEGER,
    gcr BIGINT,
    priority VARCHAR(20),
    start_lac INTEGER,
    start_ci INTEGER,
    establishment_delay INTEGER,
	sccp_success_numerator INTEGER,
	sccp_success_denominator INTEGER,
    sccp_success_rate DECIMAL(5,4),
	cell_success_numerator INTEGER,
	cell_success_denominator INTEGER,
    cell_success_rate DECIMAL(5,4),
	dispatcher_success_numerator INTEGER,
	dispatcher_success_denominator INTEGER,
    dispatcher_success_rate DECIMAL(5,4),
    end_user VARCHAR(100),
    vgcs_duration INTEGER,
	_batch_id TEXT,  
	_source_file TEXT,
    _ingested_at TIMESTAMP,
	_silver_loaded_at TIMESTAMP DEFAULT NOW(),
    _row_num INT,
    _file_hash TEXT
);

DROP TABLE IF EXISTS silver.exp_transaction_tracing CASCADE;
CREATE TABLE silver.exp_transaction_tracing(
    start_time TIMESTAMP,
    stop_time TIMESTAMP,
    call_setup_duration INTEGER,
    establishment_delay INTEGER,
    transaction_duration INTEGER,
    start_lac INTEGER,
    start_ci INTEGER,
    stop_lac INTEGER,
    stop_ci INTEGER,
    nid_engine INTEGER,
    nid_operational INTEGER,
    tmsi VARCHAR(30),
    reallocated_tmsi VARCHAR(30),
    imsi BIGINT,
    msisdn BIGINT,
    imei BIGINT,
    calling_number BIGINT,
    called_number BIGINT,
    dest_route_address VARCHAR(100),
    functional_number VARCHAR(50),
    functional_number_ct VARCHAR(100),
    direction VARCHAR(50),
    transaction_type VARCHAR(50),
    transaction_subtype VARCHAR(50),
    application_type VARCHAR(100),
    gsmr_connected VARCHAR(20),
    priority VARCHAR(20),
    root_failure VARCHAR(50),
    protocol_layer VARCHAR(50),
    end_event VARCHAR(255),
    end_cause VARCHAR(255),
    gb_ciphering_algo VARCHAR(50),
	_batch_id TEXT,  
	_source_file TEXT,
    _ingested_at TIMESTAMP,
	_silver_loaded_at TIMESTAMP,
    _row_num INT,
    _file_hash TEXT
);

DROP TABLE IF EXISTS silver.sp_ertms_disconnects CASCADE;
CREATE TABLE silver.sp_ertms_disconnects (
    nombre_ordre INT,
    derniere_7_jours BOOLEAN,
    date DATE,
    heure TIME,
    numero_train INT,
    rame INTEGER,
    motrice_cab VARCHAR(50),
    mrm INTEGER,
    imei BIGINT,
    sens VARCHAR(50),
    km DECIMAL(8,3),
    intervalle VARCHAR(20),
    niveau_etcs VARCHAR(10),
    evenement VARCHAR(255),
    cause_racine TEXT,
    analyse_smmrgv TEXT,
    sous_systeme_mis_en_cause VARCHAR(50),
    action TEXT,
    execution_ho TEXT,
    rxqual VARCHAR(50),
    rxlev VARCHAR(100),
    voisinage_10sec VARCHAR(10),
    com_dte_dce VARCHAR(10),
    ecart VARCHAR(50),
    retransmission_trames_hdlc_t70 TEXT,
    alarmes_bts VARCHAR(255),
    traite_par VARCHAR(100),
    cire VARCHAR(100),
    ciers VARCHAR(100),
    acquittement_auc TEXT,
    bug_rbc VARCHAR(100),
    liens_pai VARCHAR(255),
    pilote VARCHAR(100),
    echeance DATE,
	_batch_id TEXT,  
	_source_file TEXT,
    _ingested_at TIMESTAMP,
	_silver_loaded_at TIMESTAMP DEFAULT NOW(),
    _row_num INT,
    _file_hash TEXT
);
